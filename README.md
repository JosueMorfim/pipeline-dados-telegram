# Pipeline de Dados Telegram

Uma atividade analítica de interesse é a de realizar a análise exploratória de dados enviadas a um chatbot para responder perguntas como:

1. Qual o horário que os usuários mais acionam o bot?
2. Qual o problema ou dúvida mais frequente?
3. O bot está conseguindo resolver os problemas ou esclarecer as dúvidas?
4. Etc.

Portanto, vamos construir um pipeline de dados que ingira, processe, armazene e exponha mensagens de um grupo do Telegram para que profissionais de dados possam realizar análises. A arquitetura proposta é dividida em três: transacional, no Telegram, onde os dados são produzidos, a parte de ingestão e ETL na Amazon Web Services (AWS), e por fim a apresentação que será usado o AWS Athena para consultas SQL e o Power BI para um Dashboard interativo.

**[Link do Dashboard](https://app.powerbi.com/view?r=eyJrIjoiOGNmYThhY2YtMzk0Zi00YmFmLTk1YjItNGYzYzVkNTQ0NmNkIiwidCI6ImEwZWJjZDRhLTg0N2ItNDFjMC1iYmYyLWUzNjNkZGMzN2Y5MiJ9)**

**[Link do Notebook Kaggle](https://www.kaggle.com/code/josumorfim/pipeline-de-dados-telegram-aws-power-bi)**

![projeto telegram 2](https://github.com/JosueMorfim/Analise_Credito_SQL/assets/141301164/4c08838a-44b3-4bde-8c60-d2d0f27637d4)


