FROM node:24-alpine

WORKDIR /workspace

RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY apps/activation-api/package.json apps/activation-api/package.json
COPY apps/billing-service/package.json apps/billing-service/package.json
COPY apps/provisioning-service/package.json apps/provisioning-service/package.json
COPY apps/notification-service/package.json apps/notification-service/package.json
COPY apps/crm-analytics-service/package.json apps/crm-analytics-service/package.json
COPY apps/demo-ui/package.json apps/demo-ui/package.json
COPY libs/contracts/package.json libs/contracts/package.json
COPY libs/reliability/package.json libs/reliability/package.json
RUN pnpm install --frozen-lockfile

COPY apps ./apps
COPY libs ./libs

ARG APP
ENV APP=${APP}
RUN pnpm --filter "${APP}..." build

CMD ["sh", "-c", "pnpm --filter \"$APP\" start:prod"]
