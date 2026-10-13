# Arquitetura de dados do pacote ETL de unidades estratigráficas

## 1. Visão geral

O pacote `etl` implementa uma solução de integração e materialização de dados geoespaciais para apoiar a publicação de camadas de unidades estratigráficas em ambientes cartográficos e analíticos. A arquitetura do fluxo foi concebida como um pipeline de dados orientado a domínios temáticos, com separação clara entre:

- origem dos dados;
- enriquecimento semântico;
- materialização intermediária;
- camada de exposição pública.

A estrutura é composta por um conjunto de procedimentos PL/SQL que consolidam dados de múltiplos esquemas e tabelas, transformam atributos e geometria, e carregam os resultados em uma representação padronizada para consumo em sistemas geoespaciais.

---

## 2. Objetivos da arquitetura

A arquitetura deste pacote atende a quatro objetivos principais:

1. Integrar dados heterogêneos provenientes de diferentes estruturas de domínio;
2. Padronizar atributos e geometria para garantir coerência da camada final;
3. Materializar uma tabela de staging para controle operacional e reprocessamento;
4. Publicar uma camada final em ambiente geoserver para uso em visualização e consumo por aplicações.

Em termos de engenharia de dados, a solução funciona como um pipeline de transformação incremental com camada de staging e camada de publicação.

---

## 3. Visão lógica do fluxo

O fluxo de dados pode ser descrito em camadas:

### 3.1. Camada de origem

Dados primários de negócio e geodados são mantidos em esquemas de domínio e metadados, como:

- `litoestratigrafia.ue_geometria_unidade`
- `litoestratigrafia.ue_unidade_estratigrafica`
- `litoestratigrafia.ue_litologia`
- `litoestratigrafia.ue_auditoria_sigla`
- `bibliotecas.bb_hierarquia_estratigrafica`
- `bibliotecas.bb_range_idade`
- `geobank.wm_mapas`

Essas tabelas contêm a base estrutural sobre a qual o pacote extrai atributos, relacionamentos, classificações e geometrias.

### 3.2. Camada de processamento

A camada de processamento é composta por:

- funções de concatenação e derivação de campos;
- funções de retificação e conversão geométrica;
- procedimentos de carga em lote;
- regras de controle de integridade e sincronização.

A lógica aqui é responsável por:

- consolidar atributos semânticos;
- preparar o dado para consumo analítico;
- normalizar tipo geométrico e SRID;
- preparar a tabela materializada para publicação.

### 3.3. Camada de staging

A tabela `litoestratigrafia.ue_layer` funciona como área de staging. Ela encapsula a versão processada da camada antes da publicação. A tabela registra:

- atributos de negócio e cartografia;
- campos derivados e agregados;
- geometria já normalizada;
- metadados de controle como `record_hash`.

A staging é uma etapa crítica de qualidade e operacionalidade, permitindo reprocessamento controlado, validação intermitente e análise de defeitos sem afetar diretamente a camada pública.

### 3.4. Camada de publicação

A tabela `geoserver.ue_layer` representa a camada final exposta para consumo. Nessa camada, os registros são disponibilizados em formato compatível com ferramentas cartográficas e serviços WMS/WFS. A padronização final inclui:

- ajuste de SRID para `4326`;
- conversão para representação geográfica compatível;
- remoção de registros inválidos;
- sincronização por `record_hash` para atualização incremental.

---

## 4. Modelagem da arquitetura

A solução segue um padrão de arquitetura de dados orientado a pipeline incremental e materialização em camadas. A principal característica é a separação entre dados de origem, dados processados e dados expostos.

### 4.1. Padrão de fluxo

O fluxo pode ser resumido conforme a seguinte lógica:

1. Extração de dados de origem;
2. Transformação e enriquecimento;
3. Normalização geométrica;
4. Materialização em staging;
5. Validação operacional;
6. Sincronização para camada pública;
7. Disponibilização.

### 4.2. Dependências principais

As dependências de arquitetura mais relevantes são:

- dependência funcional de tabelas de referência e domínio;
- dependência técnica de tipos espaciais Oracle (`SDO_GEOMETRY`);
- dependência de metadados cartográficos para escala e agrupamento;
- dependência de controle de sincronização via `record_hash`.

Isso torna o pipeline sensível a mudanças de esquema e à qualidade dos dados de origem, mas também permite rastreabilidade e previsibilidade operacional.

---

## 5. Governança e qualidade de dados

### 5.1. Governança de atributos

O pacote realiza a construção de campos derivados a partir de regras de negócio e relações semânticas entre tabelas. Isso reduz a dependência direta das aplicações consumidoras sobre a estrutura física do banco e padroniza a camada final para uso analítico e cartográfico.

Exemplos de governança de atributos:

- histórico de siglas;
- agregação de litotipos;
- construção de nomes derivativos com hierarquia e unidade pai;
- composição de campos de faixa etária e escala.

### 5.2. Governança de geometria

Geometria é um componente crítico na arquitetura de dados geoespaciais. A etapa de retificação e conversão é essencial para garantir:

- integridade topológica;
- consistência espacial;
- compatibilidade com camadas de publicação;
- redução de erros em visualização e serviços de mapa.

O tratamento de geometrias inválidas e o ajuste de SRID asseguram que a camada pública mantenha padrões de qualidade mínima.

### 5.3. Controle de qualidade operacional

Os procedimentos utilizam:

- `commit` periódico em lotes;
- `FORALL` para execução em massa;
- registros de erro via `sql%bulk_exceptions`;
- validação e descarte de geometrias nulas ou incompatíveis.

Essas estratégias reduzem risco de falha total do pipeline e permitem identificar falhas de forma mais granular.

---

## 6. Padrões de integração

### 6.1. Atualização incremental

O uso de `record_hash` possibilita atualização parcial do dataset, sem necessidade de reprocessamento integral do conteúdo. Esse padrão é adequado para manter o pipeline eficiente em volumes altos de dados e reduzir custo operacional.

### 6.2. Processamento em lote

Os procedimentos usam limites explícitos para leitura e commit em blocos. Isso gera um padrão de processamento mais previsível e escalável para bases que operam com milhares de registros.

### 6.3. Heterogeneidade de fontes

A arquitetura lida com natureza heterogênea das fontes:

- diferentes esquemas;
- tabelas auxiliares com papel de domínio;
- dados espaciais em formato SDO;
- metadados cartográficos e interpretativos;
- registros históricos e de evolução do nomenclatura.

A integração ocorre não somente em nível técnico, mas também semântico, por meio de regras de derivação e padronização.

---

## 7. Mecanismos de materialização

A estratégia de materialização do pacote segue duas etapas bem definidas:

### 7.1. Materialização em staging

`litoestratigrafia.ue_layer` funciona como tabela de preparação do dado para a camada final. Ela encapsula uma visão operacional do dado em conformidade com a regra de modelagem do processo.

### 7.2. Materialização em publicação

`geoserver.ue_layer` atua como camada pública, já validada e pronta para consumo. Essa etapa separa a responsabilidade entre preparação do dado e disponibilização do dado.

A separação dessas camadas é um padrão recomendável em arquitetura de dados porque aumenta a resilência, a rastreabilidade e a capacidade de reprocesso.

---

## 8. Principais riscos arquiteturais

A arquitetura implementada apresenta alguns riscos que devem ser monitorados:

- acoplamento forte a nomes de objetos e estruturas do banco;
- sensibilidade à qualidade da geometria de origem;
- dependência de metadados de escala e mapa para correta segmentação;
- uso de consultas dinâmicas que exigem revisão em mudanças de esquema;
- potencial impacto de regressão em operações massivas de sincronização.

Esses riscos podem ser mitigados com monitoramento, documentação de schema e testes de regressão de processamento.

---

## 9. Maturidade da arquitetura

A solução está em um nível de maturidade funcional adequado para ambiente operacional de dados geoespaciais, com organização clara de etapas, materialização incremental e integração de fontes heterogêneas. No entanto, seu nível de maturidade operacional pode ser melhorado com:

- registro detalhado de logs por lote;
- métricas de volume processado e falhas por etapa;
- mensuração de tempo de processamento e throughput;
- alertas de integridade geométrica e de qualidade semântica;
- automação de reprocessamento e versionamento de camada.

---

## 10. Conclusão

A arquitetura de dados do pacote `etl` é estruturada como um pipeline de integração geoespacial em camadas, com foco em materialização, qualidade e publicação de dados. A combinação de enriquecimento semântico, padronização espacial, staging e sincronização incremental cria uma base sólida para a disponibilização de unidades estratigráficas em sistemas cartográficos e analíticos.

O desenho está alinhado com práticas de engenharia de dados modernas para bases geoespaciais: separação de responsabilidades, controle de qualidade, processamento em lote e governança da camada de produção.
