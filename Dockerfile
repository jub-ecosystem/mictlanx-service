# 
FROM python:3.9

# 
WORKDIR /app

# 
COPY ./requirements.txt /app/requirements.txt

# 
RUN pip install -r /app/requirements.txt
#
COPY ./pyproject.toml /app
COPY ./mictlanxrouter /app/mictlanxrouter
ENV MICTLANX_ROUTER_PORT=60666
ENV MICTLANX_ROUTER_HOST=0.0.0.0

# COPY ./mictlanxrouter/interfaces /app/interfaces 
# COPY ./mictlanxrouter/helpers /app/helpers

# 
# CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "60666"]
# CMD ["python3","./mictlanxrouter/server.py"]
# CMD ["sleep","infinity"]
# CMD ["uvicorn", "mictlanxrouter.server:app","--host",$MICTLANX_ROUTER_HOST,"--port",$MICTLANX_ROUTER_PORT]

