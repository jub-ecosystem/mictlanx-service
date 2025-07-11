# 
FROM python:3.10

# 
WORKDIR /app

# 
COPY ./requirements.txt /app/requirements.txt
RUN pip install --extra-index-url https://test.pypi.org/simple  -r /app/requirements.txt
#
COPY ./pyproject.toml /app
COPY ./mictlanxrouter /app/mictlanxrouter
#COPY ./mictlanx.tar.gz /app/x.tar.gz
#RUN pip install /app/x.tar.gz


ENV MICTLANX_ROUTER_PORT=60666
ENV MICTLANX_ROUTER_HOST=0.0.0.0

# COPY ./mictlanxrouter/interfaces /app/interfaces 
# COPY ./mictlanxrouter/helpers /app/helpers

# 
# CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "60666"]
# CMD ["python3","./mictlanxrouter/server.py"]
# CMD ["sleep","infinity"]
# CMD ["uvicorn", "mictlanxrouter.server:app","--host",$MICTLANX_ROUTER_HOST,"--port",$MICTLANX_ROUTER_PORT]

