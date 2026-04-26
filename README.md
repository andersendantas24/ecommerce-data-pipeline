# Pipeline E-commerce Olist — Medallion Architecture

## 1. Objetivo do Projeto

Este projeto simula um pipeline de dados para uma plataforma de e-commerce brasileira utilizando o dataset público da Olist.

O pipeline segue a arquitetura Medallion:

- Bronze: ingestão dos dados brutos em Delta Lake
- Silver: limpeza, padronização e enriquecimento dos dados
- Gold: criação de tabelas analíticas para consumo em dashboard

O projeto foi desenvolvido localmente com PySpark e Delta Lake.

---

## 2. Arquitetura do Pipeline

---

text
data/raw/
│
│ CSVs originais da Olist
│
▼
BRONZE
delta/bronze/
│
│ - Leitura dos CSVs
│ - Criação de uma tabela Delta por arquivo
│ - Adição da coluna ingestion_timestamp
│ - Preservação dos dados brutos
│
▼
SILVER
delta/silver/
│
│ - Remoção de duplicidades
│ - Conversão de colunas de data para TimestampType
│ - Remoção de registros inválidos/nulos em campos obrigatórios
│ - Filtro de pedidos válidos: delivered e shipped
│ - Criação da tabela orders_consolidated
│ - Criação da tabela payments_summary
│
▼
GOLD
delta/gold/
│
│ - customer_summary
│ - product_summary
│ - seller_summary
│
▼
SIMULAÇÃO DELTA SHARING
│
│ - Leitura das tabelas Gold
│ - Simulação de disponibilização para consumo externo

---

## 3. Decisões de Design

### 3.1. Bronze como camada bruta
Mantém os dados originais sem transformação, garantindo rastreabilidade.

### 3.2. Filtro de pedidos
Filtro de status de pedidos

Foram considerados apenas pedidos com os seguintes status:

 - delivered
 - shipped

A escolha de incluir ambos os status (e não apenas um) foi feita para equilibrar precisão analítica e representatividade do volume de vendas.

Se apenas delivered fosse considerado:

 - Teríamos uma visão mais conservadora e precisa da receita realizada
 - Porém, perderíamos pedidos que já foram enviados, mas ainda não entregues
 - Isso poderia subestimar o desempenho recente, principalmente em períodos próximos à data de extração dos dados

Se apenas shipped fosse considerado:

 - Incluiríamos pedidos que ainda podem não ser concluídos (ex: problemas na entrega)
 - Isso poderia superestimar a receita, já que nem todos os pedidos enviados são necessariamente finalizados com sucesso

Ao considerar delivered + shipped:

 - Incluímos pedidos que já estão em estágio avançado do ciclo de venda
 - Garantimos que a venda já foi efetivamente processada e despachada
Mantemos um equilíbrio entre:
 - realização da receita (delivered)
 - comprometimento da venda (shipped)

Essa abordagem é especialmente relevante em pipelines analíticos que não trabalham com dados em tempo real, pois evita distorções causadas por atrasos logísticos.

Além disso, essa decisão permite que as métricas da camada Gold reflitam melhor:

 - o volume real de vendas em andamento
 - o desempenho atual do negócio
 - sem incluir pedidos inválidos ou não concluídos

Motivo: garantir análise de vendas consistente.

Essa transformação foi aplicada na camada Silver porque:

 - A Bronze deve preservar os dados brutos, sem aplicar regras de negócio
 - A Bronze funciona como uma camada de auditoria e histórico
 - A Silver é responsável pela limpeza e aplicação de regras de negócio, como filtros e validações

Se o filtro fosse aplicado na Bronze:

 - Perderíamos dados importantes para análises futuras (ex: taxa de cancelamento)
 - Não seria possível reprocessar os dados com regras diferentes
 - Quebraria o princípio da arquitetura Medallion

### 3.3. Remoção de duplicados
A deduplicação baseada em subconjunto de colunas (ex: order_id) foi escolhida porque:

 - Em datasets transacionais, a chave de negócio (como order_id) define unicidade
 - Duplicidades podem surgir por:
    - reprocessamento de dados
    - ingestão duplicada
    - inconsistências na origem

Ao remover duplicados:

 - Evitamos inflar métricas como receita e quantidade de pedidos
 - Garantimos que cada pedido seja contabilizado apenas uma vez
 - Mantemos a consistência analítica da camada Gold


### 3.4. Receita total
Cálculo:
```
price + freight_value
```
A decisão de incluir o frete foi tomada porque:
 - O valor pago pelo cliente não é apenas o produto, mas o valor total da transação
 - Em muitos cenários de negócio, o frete faz parte do faturamento bruto
 - Ignorar o frete poderia subestimar a receita real movimentada pela plataforma

Benefícios dessa abordagem
 - Representa melhor o valor financeiro total gerado
 - Permite análises mais completas de:
    - faturamento por pedido
    - desempenho de vendedores
    - volume financeiro por produto

---

## 4. Como rodar o projeto

## 4.1. Clonar o repositório
 - git clone projeto_ecommerce 
 - cd ecommerce_test

## 4.2. Criar ambiente virtual
 - python -m venv venv

 Ativar o ambiente virtual:

 Windows:
  - venv\Scripts\activate

 Git Bash:
  - source venv/Scripts/activate

## 4.3. Instalar dependências
 - pip install -r requirements.txt

 Caso não utilize requirements.txt:
 - pip install pyspark delta-spark

## 4.4. Baixar os dados da Olist
 - Baixar o dataset Brazilian E-Commerce Public Dataset by Olist no Kaggle.

 Colocar os arquivos em:
 - data/raw/

 Arquivos esperados:
 - olist_customers_dataset.csv
 - olist_order_items_dataset.csv
 - olist_order_payments_dataset.csv
 - olist_order_reviews_dataset.csv
 - olist_orders_dataset.csv
 - olist_products_dataset.csv
 - olist_sellers_dataset.csv

## 4.5. Executar o pipeline completo
 python -m src.pipeline_runner

 Fluxo executado:
 Bronze → Silver → Gold → Simulação Delta Sharing

## 4.6. Resultado esperado
 Tabelas criadas em:
 - delta/bronze/
 - delta/silver/
 - delta/gold/

 Exemplo:
 - delta/gold/customer_summary
 - delta/gold/product_summary
 - delta/gold/seller_summary

## 4.7. Observação
 Para execução limpa, remover pasta delta:

 rm -rf delta/

 Ou deletar manualmente no Windows.


---

## 5. O que mudaria em produção

## 5.1. Orquestração com Azure Data Factory

 Na implementação local, o pipeline é executado manualmente pelo comando:

```
    bash
   python -m src.pipeline_runner

   Em um ambiente real, o fluxo seria orquestrado pelo Azure Data Factory, permitindo:

 - agendamento automático
 - controle de dependências entre etapas
 - reprocessamento em caso de falha
 - monitoramento das execuções
 - alertas para erros no pipeline

```

## 5.2. Execução e processamento no Databricks

 Neste projeto, o processamento ocorre localmente usando PySpark.

 Em produção, as transformações seriam executadas em Databricks Jobs, com clusters gerenciados e escaláveis.

 Isso permitiria:

 - maior performance
 - processamento distribuído real
 - melhor controle de recursos
 - logs centralizados
 - integração nativa com Delta Lake

## 5.3. Compartilhamento real com Delta Sharing

 Nesta implementação, o Delta Sharing foi apenas simulado lendo as tabelas Gold localmente.

 Em produção, as tabelas da camada Gold seriam compartilhadas usando Delta Sharing real, com:

 - personal access token
 - controle de acesso
 - permissões por tabela
 - compartilhamento seguro com consumidores externos
 - governança dos dados compartilhados

## 6. Limitações

- Execução local
- Sem Delta Sharing real
- Sem orquestração
- Sem carga incremental
- Validações básicas

---