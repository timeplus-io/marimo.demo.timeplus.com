CREATE OR REPLACE FUNCTION gh_api(urls string) RETURNS string LANGUAGE PYTHON AS $$
import json
import urllib.request
import urllib.parse
import urllib.error
import time
import os

# Global cache to avoid hitting rate limits
_cache = {}
_cache_ttl = {}
CACHE_DURATION = 300  # 5 minutes

def gh_api(urls):
    """
    Fetch GitHub repo data and return simplified JSON with description, stars, and language
    Input: urls is a list of strings (Timeplus converts string parameter to list)
    Output: list of JSON strings with repo info
    """
    results = []
    current_time = time.time()
    
    # Get GitHub token if available
    github_token = os.getenv('GITHUB_TOKEN')
    
    for url in urls:
        # Check cache first
        if url in _cache and url in _cache_ttl:
            if current_time - _cache_ttl[url] < CACHE_DURATION:
                results.append(_cache[url])
                continue
        
        try:
            # Create request with User-Agent header (required by GitHub API)
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'Timeplus-UDF/1.0')
            req.add_header('Accept', 'application/vnd.github.v3+json')
            
            # Add GitHub token if available
            if github_token:
                req.add_header('Authorization', f'token {github_token}')
            
            # Make the request
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.getcode() == 200:
                    # Parse the GitHub API response
                    raw_data = response.read().decode('utf-8')
                    github_data = json.loads(raw_data)
                    
                    # Extract only the fields we need
                    simplified_response = {
                        "description": github_data.get("description"),
                        "stargazers_count": github_data.get("stargazers_count", 0),
                        "language": github_data.get("language"),
                        "name": github_data.get("name"),
                        "full_name": github_data.get("full_name"),
                        "url": url,
                        "error": False
                    }
                    
                    # Handle null language
                    if simplified_response["language"] is None:
                        simplified_response["language"] = ""
                    
                    # Handle null description
                    if simplified_response["description"] is None:
                        simplified_response["description"] = ""
                    
                    result_json = json.dumps(simplified_response)
                    # Cache the successful response
                    _cache[url] = result_json
                    _cache_ttl[url] = current_time
                    results.append(result_json)
                else:
                    error_response = {
                        "error": True,
                        "status_code": response.getcode(),
                        "message": f"HTTP {response.getcode()}",
                        "url": url,
                        "description": "",
                        "stargazers_count": 0,
                        "language": "",
                        "name": "",
                        "full_name": ""
                    }
                    results.append(json.dumps(error_response))
                    
        except urllib.error.HTTPError as e:
            if e.code == 404:
                error_response = {
                    "error": True,
                    "status_code": 404,
                    "message": "Repository not found",
                    "url": url,
                    "description": "",
                    "stargazers_count": 0,
                    "language": "",
                    "name": "",
                    "full_name": ""
                }
            elif e.code == 403:
                error_response = {
                    "error": True,
                    "status_code": 403,
                    "message": "Rate limit exceeded",
                    "url": url,
                    "description": "Rate limit exceeded - add GITHUB_TOKEN",
                    "stargazers_count": 0,
                    "language": "",
                    "name": "",
                    "full_name": ""
                }
            else:
                error_response = {
                    "error": True,
                    "status_code": e.code,
                    "message": str(e),
                    "url": url,
                    "description": "",
                    "stargazers_count": 0,
                    "language": "",
                    "name": "",
                    "full_name": ""
                }
            results.append(json.dumps(error_response))
            
        except Exception as e:
            error_response = {
                "error": True,
                "status_code": 0,
                "message": f"Error: {str(e)}",
                "url": url,
                "description": "",
                "stargazers_count": 0,
                "language": "",
                "name": "",
                "full_name": ""
            }
            results.append(json.dumps(error_response))
        
        # Add small delay to avoid overwhelming the API
        if len(urls) > 1:
            time.sleep(0.1)  # 100ms delay between requests
    
    return results
$$