import json
import pymysql
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import watson_developer_cloud
from watson_developer_cloud import LanguageTranslatorV3
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud.natural_language_understanding_v1 \
  import Features, EntitiesOptions, KeywordsOptions, SentimentOptions
#----------------------------------Início das Declarações-----------------------------------------#
#-------------Your Assistant API --------------#
assistant = watson_developer_cloud.AssistantV1(
    username='xxxxxxx',#------>Your Assistant service username here
    password='xxxxxxx',#------>Your Assistant service password here
    version='2018-02-16'
)


#-------------Your NLU API --------------#
natural_language_understanding = NaturalLanguageUnderstandingV1(
  username='xxxxxx',#------>Your NLU service username here
  password='xxxxxx',#------>Your NLU service password here
  version='2018-03-16')

#-------------Your Translate API --------------#
language_translator = LanguageTranslator(
username='xxxxxxx',#------>Your Translate service username here
password='xxxxxxx')#------>Your Translate service password here

df = pd.read_excel('/MyFile.xlsx', sheet_name='Plan1')#------> Aqui você coloca o caminho do seu arquivo
valorCelula = []
translations = []
sentimento = []
celulaSentimento = []

db = pymysql.connect("localhost","root","","xxxxx" )#------>Your Local Database Connection here
cursor = db.cursor()
#----------------------------------Fim das Declarações--------------------------------------------#


#-----------------------------------Início dasFunções---------------------------------------------#


for aux2 in df.index:
    valorCelula = df['TEXTO MANIFESTACAO'][aux2]
    # Execução da tradução no Watson
    translation = language_translator.translate(
        text=valorCelula,
        model_id='pt-en')
    translations = translations + translation['translations']

    print('------------------------Tradução da Manifestação-----------------------------')
    print(json.dumps(translations[aux2], indent=2, ensure_ascii=False))

    # Execução da análise de sentimento da tradução anterior
    responseNLU = natural_language_understanding.analyze(
    text= translations[aux2]['translation'],
    language='en',
    features=Features(
        sentiment=SentimentOptions(
            )))
    #salvando o resultado da analise de sentimento no excell no excell
    df['SENTIMENTO MANIFESTACAO'][aux2] = responseNLU['sentiment']['document']['label']
    df['SCORE SENTIMENTO'][aux2] = responseNLU['sentiment']['document']['score']
    df['LINGUA MANIFESTACAO'][aux2] = responseNLU['language']

    print('------------------------Análise de Sentimento--------------------------------')
    print(json.dumps(responseNLU['sentiment']['document']['label']))
    print(json.dumps(responseNLU['sentiment']['document']['score']))
    print(json.dumps(responseNLU['language']))

    # Execução da análise para identificação de entidades
    responseWA = assistant.message(
    workspace_id='xxxxxx',#------>Your Assistant service Workspace ID here
    input={
        'text': valorCelula
        }
    )
    #salvando o resultado da identificação de entidades no excell
    if len(responseWA['entities']) <= 0:
        df['PRODUTO MANIFESTACAO'][aux2] = 'Produto Não Encontrado'
    else:
        df['PRODUTO MANIFESTACAO'][aux2] = responseWA['entities'][0]['value']

        print('------------------------Identificação de entidades---------------------------')
        print(json.dumps(responseWA['entities'][0]['value'], indent=2))

    
writer = pd.ExcelWriter('/home/capgemini/Documentos/PocDcD/python/MyFile_Full_tratado11.xlsx', engine='xlsxwriter')
df.to_excel(writer, sheet_name='Plan1', index=True)
writer.save()

for aux in df.index:
    idManifestacao = str(df['ID_MANIFESTACAO'][aux])
    agenciaCliente = str(df['AGENCIA'][aux])
    contaCliente   = str(df['CONTA'][aux])
    cpfCnpjCliente = str(df['CPF /CNPJ'][aux])
    nomeCliente    = str(df['NOME CLIENTE'][aux])
    abertManifest   = str(df['DATA ABERTURA'][aux])
    fechaManifest  = str(df['DATA CONCLUSAO'][aux])
    horaAbertura   = str(df['HORA ABERTURA'][aux])
    horaConclusao  = str(df['HORA CONCLUSAO'][aux])
    textoManifestacao = str(df['TEXTO MANIFESTACAO'][aux])
    canalManifestacao = str(df['CANAL MANIFESTACAO'][aux])
    sentiManifestacao = str(df['SENTIMENTO MANIFESTACAO'][aux])
    scoreSentimento   = str(df['SCORE SENTIMENTO'][aux])
    linguaManifestacao = str(df['LINGUA MANIFESTACAO'][aux])
    produtoManifestacao = str(df['PRODUTO MANIFESTACAO'][aux])
    
    sql = "INSERT INTO dcd_analise(idManifest, agCliente, contaCliente, cpfcnpjCliente, nomeCliente, abertManifest, fechaManifest, hrAbertManifest, hrFechaManifest, textoManifest, canalManifest, senti_manifest, score_senti, lang_manifest, produtoManifest) VALUES ('"+idManifestacao+"','"+agenciaCliente+"','"+contaCliente+"','"+cpfCnpjCliente+"','"+nomeCliente+"','"+abertManifest+"','"+fechaManifest+"','"+horaAbertura+"','"+horaConclusao+"','"+textoManifestacao+"','"+canalManifestacao+"','"+sentiManifestacao+"','"+scoreSentimento+"','"+linguaManifestacao+"','"+produtoManifestacao+"')"
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()

db.close()