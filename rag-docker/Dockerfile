# 1. 基础镜像：自带 Python 3.12 的精简 Linux
FROM python:3.12-slim

# 2. 设工作目录
WORKDIR /rag-app

# 3. 换 Debian 国内源（阿里云），加速 apt 下载
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources

# 4. 装 Java（PySpark 需要 JDK）+ 清理 apt 缓存减小镜像体积
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk \
    && rm -rf /var/lib/apt/lists/*

# 4. Java 环境变量（PySpark 要读）
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 5. 先拷依赖文件，单独装包（利用 Docker 层缓存——代码改了不需重装包）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 6. 再拷代码
COPY pipeline/ pipeline/
COPY api_server.py .

# 7. 创建数据和输出目录
RUN mkdir -p /rag-app/data /rag-app/output

# 8. 默认执行主入口
CMD ["python", "pipeline/main.py"]
