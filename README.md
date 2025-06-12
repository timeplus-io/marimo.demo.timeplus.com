# marimo.demo.timeplus.com
A set of public demos using [marimo](https://marimo.io) Python notebook.

## How to run locally
Access https://marimo.demo.timeplus.com to see the demos. The website is served by ngnix and fastapi with marimo notebooks.

To run the notebooks locally, you can use the following commands:
```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Run the FastAPI server to host marimo notebooks
uv run main.py
```

To create a new notebook or edit an existing one, you can use the following command:
```bash
uv run marimo edit notebook.py
```

## How the website is built

```bash
sudo apt install nginx git

git clone https://github.com/timeplus-io/marimo.demo.timeplus.com.git
cd marimo.demo.timeplus.com
# start fastapi server in the background
nohup uv run main.py > marimo.log 2>&1 &

sudo vim /etc/nginx/conf.d/marimo.conf
# after editing the file, run
sudo nginx -s reload
```

### marimo.conf
```nginx
server {
    listen 443 ssl http2;
    ssl_certificate /etc/letsencrypt/live/marimo.demo.timeplus.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/marimo.demo.timeplus.com/privkey.pem;
    server_name marimo.demo.timeplus.com;

    location / {
        proxy_set_header    Host $host;
        proxy_set_header    X-Real-IP $remote_addr;
        proxy_set_header    X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header    X-Forwarded-Proto $scheme;
        proxy_pass          http://127.0.0.1:8080;

        # Required for WebSocket support
        proxy_http_version  1.1;
        proxy_set_header    Upgrade $http_upgrade;
        proxy_set_header    Connection "upgrade";
        proxy_read_timeout  600;
    }
}
```

### How to update the demo website
Make change locally and push to the `main` branch.
On the remote server, run the following commands:
```bash
cd /path/to/marimo.demo.timeplus.com
git pull origin main
# find the background process id
ps aux | grep marimo
# kill the process
kill -9 <pid>
# restart the fastapi server
nohup uv run main.py > marimo.log 2>&1 &
```
