FROM node:20-alpine

# create working directory
WORKDIR /app

# install dependencies first (better caching)
COPY package*.json ./

RUN npm install

# copy source
COPY . .

# run pipeline
CMD ["node", "pipeline.js"]