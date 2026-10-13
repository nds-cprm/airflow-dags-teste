# Documentação de engenharia de dados do pacote ETL

## Visão geral

Este pacote Oracle implementa um pipeline de transformação e consolidação de dados geoespaciais para produção de uma camada analítica de unidades estratigráficas. O fluxo integra dados do esquema `litoestratigrafia`, metadados de classificação e mapas do esquema `bibliotecas`/`geobank`, e materializa uma camada padronizada em `litoestratigrafia.ue_layer`, seguida da sincronização para `geoserver.ue_layer`.

O processo contempla:

- enriquecimento dimensional de atributos;
- composição de campos derivativos para consumo analítico;
- padronização de tipos geométricos e SRID;
- processamento em lote para aumento de throughput;
- atualização incremental e controle de consistência entre camada staging e camada publicada.

A arquitetura do pacote é orientada a pipelines ETL em PL/SQL, com foco em reprodutibilidade, rastreabilidade e preparação do dado para consumo em aplicações cartográficas e analíticas.

---

## Objetivo do pipeline

O objetivo funcional do pacote é disponibilizar uma base de unidades estratigráficas harmonizada e pronta para uso em camadas geográficas. A estrutura final agrega:

- identificador da unidade estratigráfica;
- sigla e nome canônico;
- hierarquia estratigráfica e unidade parental;
- ambiente tectônico, subambiente e classificação de escala;
- faixa de idade e nomenclatura geocronológica;
- litotipos e atributos complementares;
- histórico de siglas;
- geometria espacial em formato adequado para publicação.

A saída é uma camada materializada com qualidade de dados suficiente para suporte analítico, visualização geoespacial e integração com serviços de mapa.

---

## Arquitetura do pacote

### 1. Camada de funções utilitárias

#### `monta_siglas_historicas(p_id_unidade_estratigrafica in number) return varchar2`

Essa função implementa a genealogia de alteração de siglas para uma unidade estratigráfica. O mecanismo:

- monta uma instrução SQL dinâmica;
- filtra `ue_auditoria_sigla` por `id_unidade_estratigrafica`;
- seleciona registros de `sigla_anterior` com `comando = 'alteracao'`;
- concatena os valores em um campo textual ordenado, delimitado por `, `.

O resultado é incorporado ao atributo `siglas_historicas`, que serve como suporte para rastreabilidade histórica e governança de nomenclatura.

#### `monta_string(p_parent_table in varchar2, p_parent_join_column in varchar2, p_filter_column in varchar2, p_alternative_column in varchar2, p_child_table in varchar2, p_id in number) return varchar2`

A função compõe campos agregados a partir de relações entre tabelas pai-filho. Ela produz um texto concatenado para representar uma lista de valores associados a uma entidade dominante.

Casos de uso típicos no pacote:

- composição de litotipos;
- derivação de valores de classificação;
- materialização de listas de domínio voltadas a visualização e consumo analítico.

O padrão utiliza `select distinct` para evitar duplicidade semântica e `order by 1` para estabilidade da geração do campo derivado.

### 2. Camada de padronização geométrica

#### `to_multipolygon(g in sdo_geometry) return sdo_geometry`

Função de normalização geométrica que converte um polígono simples em um multipolígono quando o tipo de geometria de origem exige essa representação. O comportamento é:

- valida o `sdo_gtype` da entrada;
- preserva os metadados de estrutura `elem_info`;
- reconstitui a geometria em um formato compatível com a camada de publicação.

Essa etapa torna o dado homogêneo em termos de estrutura geométrica e reduz falhas de compatibilidade em serviços de mapa e modelos de geoprocessamento.

#### `rectify_geometry(g in sdo_geometry) return sdo_geometry`

A função retifica geometria via operações espaciais em Oracle Spatial, ajustando a estrutura da forma antes da publicação. O fluxo aplica:

- extração de anéis por `sdo_util.extract`;
- agregação dos anéis em uma topologia linear;
- união espacial por `sdo_geom.sdo_union`;
- reconstrução dos polígonos por `sdo_geom.sdo_buffer`.

Se a geometria final não se enquadra em tipo válido (`2003` ou `2007`), a função retorna a geometria original para preservar o dado bruto e reduzir perdas de informação.

---

## Pipeline ETL

### `carrega_layer(p_layer in varchar2)`

Este procedimento é o ponto central do pipeline de materialização. Ele define cursoras por faixa de escala e por categoria de mapa, extraindo dados de origem e preparando a camada analítica.

Cursores principais:

- `c_unidades_estados_all`
- `c_unidades_all(7)`
- `c_unidades_all(6)`
- `c_unidades_all(5)`
- `c_unidades_all(17)`
- `c_unidades_all(4)`
- `c_unidades_1000000_all`
- `c_unidades_2500000_all`

Cada cursor:

- lê registros em `litoestratigrafia.ue_geometria_unidade`;
- faz joins com entidades de domínio e metadados geográficos;
- constrói campos de camada;
- deriva `record_hash` a partir de `gu.rowid` para controle de atualização.

Fluxo operacional:

1. reset da sequence `r169323`;
2. truncamento da tabela `litoestratigrafia.ue_layer`;
3. inserção em lote por cursor;
4. commit periódico a cada 1000 registros.

O procedimento é responsável por preparar os dados em um formato de staging analítico, pronto para o processo de publicação.

### `carrega_layers`

Procedimento de orquestração do fluxo principal:

- `carrega_layer('UE_LAYER');`

### `carrega_layer_geoserver`

Este procedimento materializa os registros para `geoserver.ue_layer` e implementa o passo final de disponibilização. A rotina realiza:

1. atualização de `fid` pela chave de relacionamento entre origem e destino;
2. leitura em lotes com limite de 100 registros;
3. conversão de WKB/SDE para `SDO_GEOMETRY` via `sde.st_asbinary` e `sdo_util.from_wkbgeometry`;
4. retificação da geometria com `sdo_util.rectify_geometry`;
5. ajuste do `SRID` para `4326`;
6. insert em lote em `geoserver.ue_layer`;
7. commit por batch;
8. exclusão de registros com geometria nula.

O código também registra falhas usando `sql%bulk_exceptions` e `dbms_output.put_line` para observabilidade de processamento.

### `converte_layer_to_multipolygon`

Procedimento de normalização de schema geométrico. Ele identifica geometrias em formato `2003` e as converte para `2007` com `to_multipolygon` para harmonizar a camada antes da publicação em infraestrutura cartográfica.

### `update_layer_geometries`

Procedimento de atualização incremental de geometrias da camada pública. Ele:

- lê `record_hash` e geometria retificada em lote;
- ajusta `SRID` para 4326;
- executa updates massivos via `FORALL`;
- coleta erros via `sql%bulk_exceptions` para tratamento estruturado.

Este padrão reduz a necessidade de reprocessamento completo e melhora a eficiência do pipeline.

---

## Fluxo de dados e dependências

1. Dados brutos em `litoestratigrafia.ue_geometria_unidade` e tabelas adjacentes.
2. Derivação de atributos e composição de campos analíticos.
3. Materialização em `litoestratigrafia.ue_layer`.
4. Normalização espacial e ajuste de tipo geométrico.
5. Carga em `geoserver.ue_layer`.
6. Disponibilização para consumo em serviços e layers cartográficos.

A cadeia de processamento apresenta um desenho clássico de pipeline ELT/ETL com pequenas variações: a tabela intermediária funciona como layer de staging, enquanto a tabela de geoserver realiza a camada de exposição.

---

## Princípios de qualidade e governança de dados

### `record_hash` como chave de integração

`record_hash` atua como identificador de sincronização entre as camadas staging e publicada. Esse mecanismo proporciona:

- consistência incremental;
- detecção de registros já processados;
- atualização sem reprocessamento completo da tabela;
- redução do risco de duplicidade de registros.

### Lotes e processamento incremental

O pacote implementa carregamento em lotes para mitigar custos de I/O e reduzir pressão de memória em operações massivas. A estratégia de `fetch` e `commit` por blocos é uma convenção adequada para processamento de dados volumosos em Oracle.

### Padronização espacial

A normalização de geometria é um aspecto crítico do pipeline. Geometrias inconsistentes podem produzir:

- falhas de conversão;
- incompatibilidades com o serviço de mapas;
- erros de renderização e publicação;
- degradação da qualidade das camadas analíticas.

Nesse sentido, a retificação, ajuste de SRID e conversão de tipos funcionam como mecanismos de qualidade estrutural.

### Sequenciamento e idempotência

A sequence `r169323` é reinicializada antes da carga para manter integridade do identificador e reduzir riscos de colisão. Em pipelines de dados, essa prática é importante para controle de reprocessamento e consistência de tabela materializada.

---

## Observações de engenharia

### Dependência do Oracle Spatial

O pacote depende diretamente de funções e tipos do Oracle Spatial, principalmente:

- `SDO_GEOMETRY`
- `SDO_UTIL.extract`
- `SDO_UTIL.from_wkbgeometry`
- `SDO_UTIL.rectify_geometry`
- `SDO_GEOM.sdo_union`
- `SDO_GEOM.sdo_buffer`

Esse conjunto de operações exige atenção aos seguintes aspectos:

- consistência de `sdo_elem_info`;
- `sdo_ordinates` válidos;
- tipos de geometria esperados;
- tolerância de precisão espacial.

### SQL dinâmico

O pacote usa SQL dinâmico em funções de agregação textual. Isso aumenta flexibilidade de composição, mas também exige rigor na validação de nomes de tabela, coluna e estruturas de relacionamento. Alterações de esquema ou renome de objetos podem impactar diretamente a execução do pipeline.

### Tratamento de exceções

Há mecanismos defensivos para falhas de geometria e processamento em massa. No entanto, a rotina ainda depende de insumos consistentes e de um pipeline de origem estável para evitar regressões operacionais.

---

## Entidades e tabelas do pipeline

### Tabelas de origem

- `litoestratigrafia.ue_geometria_unidade`
- `litoestratigrafia.ue_unidade_estratigrafica`
- `litoestratigrafia.ue_litologia`
- `litoestratigrafia.ue_auditoria_sigla`
- `bibliotecas.bb_hierarquia_estratigrafica`
- `bibliotecas.bb_range_idade`
- `geobank.wm_mapas`

### Tabelas de staging e publicação

- `litoestratigrafia.ue_layer`
- `geoserver.ue_layer`

---

## Riscos e pontos de atenção

- acoplamento forte com estruturas de esquema e nomes de objetos;
- dependência de qualidade topológica das geometrias de origem;
- processamento sensível a variações de escala e filtros de mapa;
- uso de SQL dinâmico que exige revisão de compatibilidade em mudanças de modelagem;
- necessidade de monitoramento de performance em execução em grandes volumes de dados.

---

## Resumo arquitetural

O pacote implementa um fluxo de ETL geoespacial em PL/SQL para transformar dados geológicos em uma camada analítica e publicável. A arquitetura combina enriquecimento semântico, integração dimensional, padronização de geometria e materialização incremental para servir sistemas cartográficos de alta disponibilidade. Em termos de engenharia de dados, ele se caracteriza por um desenho de staging + publicação, controle de qualidade e otimização de processamento em lote, com ênfase em consistência, rastreabilidade e distribuição do dado para serviços de visualização.

---

## Recomendações de melhoria

Para fortalecer a maturidade do pipeline, recomenda-se:

- padronizar metadados e documentação de cada cursor e procedimento;
- implementar logs estruturados por batch e por erro geométrico;
- validar regras de qualidade antes da carga em produção;
- introduzir monitoramento de performance e volume por etapa do pipeline;
- centralizar regras de transformação em funções e views reutilizáveis;
- estabelecer política de reprocessamento e versionamento de camadas materializadas.
