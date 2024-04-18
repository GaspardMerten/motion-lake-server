FROM python:3.12

# Copy all the files from the project to the container (except the ones in .dockerignore)
COPY . /app

# Set the working directory
WORKDIR /app

# Install the dependencies
RUN pip install -r requirements.txt

# RUN THE cli.py FILE  with all the arguments (from environment variables)
CMD ["/app/launcher.sh"]