# Выкачиваем из Docker Hub образ с Python версии 3.11
FROM python:3.11

# Устанавливаем рабочую директорию для проекта в контейнере
WORKDIR /backend

# Отключаем кэширование байт-кода и включаем буферизацию вывода
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Копируем файлы зависимостей и устанавливаем их
COPY requirements.txt /backend/
RUN pip install --upgrade pip && \
    pip install -r requirements.txt --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org

# Копируем все файлы проекта в контейнер
COPY . /backend

# Открываем порт 8080
EXPOSE 8080

# Запускаем приложение
ENTRYPOINT ["python", "async_public.py"]
