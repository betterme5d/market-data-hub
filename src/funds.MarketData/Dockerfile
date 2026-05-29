FROM python:3.11-slim

WORKDIR /app

# 安装必要的系统工具
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 升级 pip 并使用镜像源
RUN pip install --upgrade pip -i https://mirrors.aliyun.com/pypi/simple/

# 复制依赖并安装，增加镜像源和超时时间
COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# 复制源代码
COPY . .

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
