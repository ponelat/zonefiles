FROM node

RUN mkdir -p /usr/app/
COPY ./package.json /usr/app/package.json
WORKDIR /usr/app
RUN npm install --production
COPY . /usr/app/

CMD ["npm", "start"]
