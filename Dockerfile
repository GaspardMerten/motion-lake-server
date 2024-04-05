FROM python

# Copy all the files from the project to the container
COPY . /app

# Set the working directory
WORKDIR /app

# Install the dependencies
RUN pip install -r requirements.txt

# RUN THE cli.py FILE  with all the arguments (from environment variables)
CMD ["python", "cli.py"]
