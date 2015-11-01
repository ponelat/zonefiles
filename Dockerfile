FROM node

RUN mkdir -p /usr/app/
COPY . /usr/app
WORKDIR /usr/app
RUN npm install --production

CMD ["npm", "start"]
