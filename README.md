# Processamento do Log das Urnas 

Este repositório contém scripts pySpark (Python + Spark) para processamento dos logs das urnas eletrônicas.
Extrai informações sobre o tempo de votação, tempo de habilitação, número de tentativas de identificação biométrica, juntamente a dados sobre a seção, uf, zona e outros dados cadastrais de cada voto computado.

## Os dados
Os logs de urna podem ser baixados diretamente no [site de dados abertos TSE](https://dadosabertos.tse.jus.br/dataset/resultados-2022-arquivos-transmitidos-para-totalizacao).
Este repositório contém scripts python que fazem o download e extração automática dos logs.

## O que são os logs da Urna eletrônica?

O Log de uma urna é um arquivo que contém todas as operações realizadas na urna, desde a carga inicial até o final da votação no segundo turno (se houver).
Os arquivos são armazenados em texto simples, com cada linha representando um evento. Veja um exemplo abaixo:

```
21/09/2022 17:21:41	INFO	67305985	LOGD	Início das operações do logd	FDE9B0FC7A079096
21/09/2022 17:21:41	INFO	67305985	LOGD	Urna ligada em 21/09/2022 às 17:20:16	B637C17E565B039B
21/09/2022 17:21:41	INFO	67305985	SCUE	Iniciando aplicação - Oficial - 1º turno	F82E007ACCAF93A5
21/09/2022 17:21:41	INFO	67305985	SCUE	Versão da aplicação: 8.26.0.0 - Onça-pintada	D499E9A173814A70
```

Com eles, é possível extrair inúmeros informações sobre o processo eleitoral. Exatamente por sua verbosidade, os logs das urnas são relativamente pesados. Em formato original, o conjunto de arquivos de log de uma UF pode variar de 2G até mais de 50G. Por isso, ferramentas de processamento mais robustas e formatos de arquivo mais otimizados são indispensáveis.

## Como executar o projeto?
Caso queira instalar o ambiente manualmente em máquina local - 
Requisitos:
- Python 3.8+
- Biblioteca pyspark 
- [Apache Spark](https://spark.apache.org/) 3.3.0

(Recomendado) O Arquivo docker-compose.yaml contém a configuração necessária para executar o projeto usando docker.
Requisitos:
- Docker
- Docker-compose

Iniciando os containers:
```
docker-compose up
```

Visualizando os containers
```
docker ps
```

Para executar um script, basta utilizar o comando spark-submit
```
spark-submit <arquivo.py>
```
Nota: Caso esteja usando docker, este comando deve ser executado internamente no container.

## Nota sobre aproximações e erros
Processar os logs das urnas não é uma tarefa simples.
Embora sejam simples de ler, definir um processo que isole perfeitamente cada voto é uma tarefa complexa, pois inúmeras situações podem ocorrer durante a votação.

Os scripts codificados aqui tentam ser o mais genéricos e simples o possível, para facilitar a compreensão, manutenção e diminuir o custo computacional do processamento. Por isso, eventualmente podem não capturar perfeitamente TODOS os votos. A taxa de erro (votos não capturados) considerando contagem oficial do TSE é de ~3% (experimento feito com dados do RN)


