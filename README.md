# tipagemdecolunas
Esse repositório tem dois tipos de código. Um para forçagem de tipagem em partições e outra para conversão de arquivo .csv para .parquet.
o arquivo que faz conversão de .csv para .parqute utiliza a biblioteca Pandas para a manipulação dos arquivos e o boto3 para se comunicar com a AWS.
também faz a forçagem de tipos para uma sequencia de colunas e no final, faz o upload do arquivo para um bucket do Amazon s3.
