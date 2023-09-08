import awswrangler as wr
import boto3

def create_athena_table(database, table_name, sql_file_path, output_location):
    # Inicialize o cliente do Athena
    athena_client = boto3.client('athena', region_name='sua_regiao')

    # Ler o arquivo SQL
    with open(sql_file_path, 'r') as sql_file:
        create_table_sql = sql_file.read()

    # Executar a consulta SQL para criar a tabela no Athena
    wr.athena.execute_query(
        sql=create_table_sql,
        database=database,
        output_location=output_location,
        athena_client=athena_client
    )

if __name__ == "__main__":
    # Defina os parâmetros da tabela e do arquivo SQL
    database_name = "seu_banco_de_dados"
    table_name = "sua_tabela"
    sql_file_path = "caminho/para/seu/arquivo.sql"
    output_location = "s3://seu-bucket-de-saida/"

    # Chame a função para criar a tabela
    create_athena_table(database_name, table_name, sql_file_path, output_location)

# --------------------------------------------------------------------------------------------

import boto3

def delete_s3_file(bucket_name, file_key):
    try:
        # Inicialize o cliente S3
        s3_client = boto3.client('s3')

        # Exclua o arquivo
        s3_client.delete_object(Bucket=bucket_name, Key=file_key)

        print(f"Arquivo s3://{bucket_name}/{file_key} excluído com sucesso.")
    except Exception as e:
        print(f"Erro ao excluir o arquivo s3://{bucket_name}/{file_key}: {str(e)}")

if __name__ == "__main__":
    # Defina o nome do bucket e a chave do arquivo que deseja excluir
    bucket_name = "seu-bucket"
    file_key = "caminho/para/seu/arquivo.txt"

    # Chame a função para excluir o arquivo
    delete_s3_file(bucket_name, file_key)

# ------------------------------------------------------------------------------------

import boto3

def update_lake_formation_grants(resource_arn, principal_arn, database, table, permissions, grant_option=False):
    """
    Atualiza os grants no AWS Lake Formation.

    :param resource_arn: ARN do recurso de dados (banco de dados ou tabela).
    :param principal_arn: ARN do principal para o qual as permissões são concedidas.
    :param database: Nome do banco de dados (caso aplicável).
    :param table: Nome da tabela (caso aplicável).
    :param permissions: Lista de permissões a serem concedidas/revogadas (por exemplo, ["SELECT", "INSERT"]).
    :param grant_option: Indica se o GRANT OPTION deve ser concedido (para permitir que o principal conceda permissões a outros).
    """
    lakeformation_client = boto3.client('lakeformation', region_name='sua_regiao')

    # Construir a estrutura de permissões
    permission_list = [{'Principal': principal_arn, 'Actions': permissions}]
    
    # Se grant_option for True, adicionar a opção GRANT
    if grant_option:
        for perm in permission_list:
            perm['GrantOption'] = True

    try:
        # Conceder ou revogar permissões no recurso de dados especificado
        response = lakeformation_client.grant_permissions(
            Principal=principal_arn,
            Resource=resource_arn,
            PermissionsWithGrantOption=permission_list
        )
        print(f"Permissões atualizadas com sucesso para {resource_arn} por {principal_arn}.")
    except Exception as e:
        print(f"Erro ao atualizar permissões: {str(e)}")

if __name__ == "__main__":
    # Definir os parâmetros
    resource_arn = "ARN_DO_SEU_RECURSO"  # ARN do banco de dados ou tabela
    principal_arn = "ARN_DO_PRINCIPAL"  # ARN do principal ao qual você deseja conceder permissões
    database_name = "NOME_DO_BANCO_DE_DADOS"
    table_name = "NOME_DA_TABELA"
    permissions = ["SELECT", "INSERT"]  # Lista de permissões a serem concedidas
    grant_option = False  # Se True, concederá a opção GRANT

    # Chamar a função para atualizar as permissões
    update_lake_formation_grants(resource_arn, principal_arn, database_name, table_name, permissions, grant_option)

# -----------------------------------------------------------------------------------------

import boto3

def get_role_arn_by_name(role_name):
    # Crie um cliente IAM
    iam_client = boto3.client('iam')

    try:
        # Liste as funções IAM para encontrar a função com o nome especificado
        response = iam_client.list_roles()
        roles = response.get('Roles', [])

        for role in roles:
            if role['RoleName'] == role_name:
                return role['Arn']
    except Exception as e:
        print(f"Erro ao buscar a função {role_name}: {str(e)}")

    return None

if __name__ == "__main__":
    # Defina o nome da função IAM que você deseja usar
    role_name = "NomeDaSuaFuncaoIAM"

    # Obtenha o ARN da função usando o nome
    principal_arn = get_role_arn_by_name(role_name)

    if principal_arn:
        # Agora você pode usar principal_arn em sua função original
        print(f"ARN da função {role_name}: {principal_arn}")
    else:
        print(f"Função {role_name} não encontrada.")

# -----------------------------------------------------------------------------

import boto3

def update_lake_formation_grants_with_role(resource_arn, role_name, database, table, permissions, grant_option=False):
    """
    Atualiza os grants no AWS Lake Formation usando uma função IAM.

    :param resource_arn: ARN do recurso de dados (banco de dados ou tabela).
    :param role_name: Nome da função IAM que concederá permissões.
    :param database: Nome do banco de dados (caso aplicável).
    :param table: Nome da tabela (caso aplicável).
    :param permissions: Lista de permissões a serem concedidas/revogadas (por exemplo, ["SELECT", "INSERT"]).
    :param grant_option: Indica se o GRANT OPTION deve ser concedido (para permitir que a função conceda permissões a outros).
    """
    lakeformation_client = boto3.client('lakeformation', region_name='sua_regiao')

    # Construir a estrutura de permissões
    permission_list = [{'Principal': role_name, 'Actions': permissions}]
    
    # Se grant_option for True, adicionar a opção GRANT
    if grant_option:
        for perm in permission_list:
            perm['GrantOption'] = True

    try:
        # Conceder ou revogar permissões no recurso de dados especificado
        response = lakeformation_client.grant_permissions(
            Principal=role_name,
            Resource=resource_arn,
            PermissionsWithGrantOption=permission_list
        )
        print(f"Permissões atualizadas com sucesso para {resource_arn} usando a função IAM {role_name}.")
    except Exception as e:
        print(f"Erro ao atualizar permissões: {str(e)}")

if __name__ == "__main__":
    # Definir os parâmetros
    resource_arn = "ARN_DO_SEU_RECURSO"  # ARN do banco de dados ou tabela
    role_name = "NomeDaSuaFuncaoIAM"  # Nome da função IAM que concederá permissões
    database_name = "NOME_DO_BANCO_DE_DADOS"
    table_name = "NOME_DA_TABELA"
    permissions = ["SELECT", "INSERT"]  # Lista de permissões a serem concedidas
    grant_option = False  # Se True, concederá a opção GRANT

    # Chamar a função para atualizar as permissões usando a função IAM
    update_lake_formation_grants_with_role(resource_arn, role_name, database_name, table_name, permissions, grant_option)

# --------------------------------------------------------------------------------------

import awswrangler as wr
import boto3

def execute_athena_query_and_save_to_s3(database, sql_file_path, s3_output_path):
    # Inicialize o cliente do Athena
    athena_client = boto3.client('athena', region_name='sua_regiao')

    # Ler o arquivo SQL
    with open(sql_file_path, 'r') as sql_file:
        sql_query = sql_file.read()

    try:
        # Executar a consulta no Athena
        df = wr.athena.read_sql_query(
            sql_query,
            database=database,
            ctas_approach=False,
            athena_client=athena_client
        )

        # Salvar o resultado no Amazon S3
        wr.s3.to_csv(
            df=df,
            path=s3_output_path,
            index=False,
            dataset=True,
            mode='overwrite',
            database=database,
            table='nome_da_tabela_no_s3'  # Nome da tabela S3
        )

        print(f"Resultado da consulta salvo em {s3_output_path}")
    except Exception as e:
        print(f"Erro ao executar a consulta e salvar no S3: {str(e)}")

if __name__ == "__main__":
    # Defina os parâmetros da consulta e local de saída no S3
    database_name = "seu_banco_de_dados"
    sql_file_path = "caminho/para/seu/arquivo.sql"
    s3_output_path = "s3://seu-bucket/de_saida/pasta/"

    # Chame a função para executar a consulta e salvar no S3
    execute_athena_query_and_save_to_s3(database_name, sql_file_path, s3_output_path)

# -----------------------------------------------------------------------------------

import boto3

def execute_glue_job(job_name):
    """
    Executa um job do AWS Glue.

    :param job_name: Nome do job Glue a ser executado.
    """
    glue_client = boto3.client('glue', region_name='sua_regiao')

    try:
        response = glue_client.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        print(f"Job {job_name} iniciado com JobRunId: {job_run_id}")
    except Exception as e:
        print(f"Erro ao iniciar o job {job_name}: {str(e)}")

if __name__ == "__main__":
    # Defina o nome do job Glue que você deseja executar
    job_name = "NomeDoSeuJobGlue"

    # Chame a função para executar o job
    execute_glue_job(job_name)
