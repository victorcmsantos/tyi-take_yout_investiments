FROM node:20-alpine AS build

WORKDIR /app

ARG VITE_OPENCLAW_ENRICHMENT_STALE_DAYS=3
ENV VITE_OPENCLAW_ENRICHMENT_STALE_DAYS=$VITE_OPENCLAW_ENRICHMENT_STALE_DAYS

COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM nginx:1.27-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 80

CMD ["/docker-entrypoint.sh"]
