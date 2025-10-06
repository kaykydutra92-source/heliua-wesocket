FROM node:20-alpine

WORKDIR /app

# small tools for debugging if you exec into the container
RUN apk add --no-cache bash curl

COPY package*.json ./
RUN npm install --production

# copy source
COPY . .

EXPOSE 3000
CMD ["node", "node-app.js"]