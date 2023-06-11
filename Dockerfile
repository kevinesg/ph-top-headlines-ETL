FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --no-cache-dir

COPY ./credentials ./credentials
COPY ./flows ./flows

CMD prefect cloud login \
 --key ENTER_YOUR_PREFECT_CLOUD_LOGIN_KEY_HERE \
 --workspace USERNAME/WORKSPACE_NAME; \
 prefect work-pool create --type prefect-agent WORKPOOL_NAME; \
 prefect work-pool set-concurrency-limit WORKPOOL_NAME 1; \
 prefect deployment build flows/main_flow.py:main_flow \
 --name ph_news_main_deployment_1 \
 --pool WORKPOOL_NAME \
 --work-queue default \
 --cron "0 0-21/3 * * *" \
 --timezone "Asia/Manila"; \
 mv main_flow-deployment.yaml main_flow-deployment_1.yaml; \
 prefect deployment apply main_flow-deployment_1.yaml; \
 prefect deployment build flows/main_flow.py:main_flow \
 --name ph_news_main_deployment_2 \
 --pool WORKPOOL_NAME \
 --work-queue default \
 --cron "30 1-22/3 * * *" \
 --timezone "Asia/Manila"; \
 mv main_flow-deployment.yaml main_flow-deployment_2.yaml; \
 prefect deployment apply main_flow-deployment_2.yaml; \
 prefect agent start --pool WORKPOOL_NAME --work-queue default