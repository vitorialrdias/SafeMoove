import io
import json
import os
import shutil
import time
import zipfile

import boto3

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
FUNCTION_NAME = "safemoove-refresh-gold"
ROLE_NAME = "safemoove-refresh-gold-role"
RULE_NAME = "safemoove-refresh-gold-schedule"
SCHEDULE = "rate(30 minutes)"
DATA_BUCKET = "safe-moove-raw"

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
GOLD_SQL_DIR = os.path.join(REPO_ROOT, "infra", "athena", "gold")

sts = boto3.client("sts", region_name=REGION)
ACCOUNT_ID = sts.get_caller_identity()["Account"]
RESULTS_BUCKET = f"safemoove-athena-results-{ACCOUNT_ID}"

iam = boto3.client("iam", region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
}

PERMISSIONS_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            "Resource": "arn:aws:logs:*:*:*",
        },
        {
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
            ],
            "Resource": "*",
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
                "glue:CreateTable", "glue:DeleteTable", "glue:UpdateTable",
                "glue:GetPartitions", "glue:BatchCreatePartition",
            ],
            "Resource": "*",
        },
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
            "Resource": [f"arn:aws:s3:::{DATA_BUCKET}", f"arn:aws:s3:::{DATA_BUCKET}/*"],
        },
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
            "Resource": [f"arn:aws:s3:::{RESULTS_BUCKET}", f"arn:aws:s3:::{RESULTS_BUCKET}/*"],
        },
    ],
}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(os.path.join(HERE, "handler.py"), "handler.py")
        for nome_arquivo in ("create-onibus-por-dia-tipo.sql", "create-atraso-por-linha.sql"):
            origem = os.path.join(GOLD_SQL_DIR, nome_arquivo)
            zf.write(origem, nome_arquivo)
    buf.seek(0)
    return buf.read()


def ensure_role():
    try:
        role = iam.get_role(RoleName=ROLE_NAME)
        print(f"[OK] role {ROLE_NAME} ja existe")
        iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName="safemoove-refresh-gold-policy",
            PolicyDocument=json.dumps(PERMISSIONS_POLICY),
        )
        return role["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        pass

    role = iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
        Description="Execucao da Lambda que recria as tabelas gold do SafeMoove",
    )
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="safemoove-refresh-gold-policy",
        PolicyDocument=json.dumps(PERMISSIONS_POLICY),
    )
    print(f"[OK] role {ROLE_NAME} criada")
    print("aguardando propagacao do IAM...")
    time.sleep(10)
    return role["Role"]["Arn"]


def ensure_function(role_arn, zip_bytes):
    env = {
        "Variables": {
            "ATHENA_RESULTS_BUCKET": RESULTS_BUCKET,
            "DATA_BUCKET": DATA_BUCKET,
        }
    }

    try:
        lambda_client.get_function(FunctionName=FUNCTION_NAME)
        lambda_client.update_function_code(FunctionName=FUNCTION_NAME, ZipFile=zip_bytes)
        waiter = lambda_client.get_waiter("function_updated")
        waiter.wait(FunctionName=FUNCTION_NAME)
        lambda_client.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Role=role_arn,
            Timeout=180,
            MemorySize=256,
            Environment=env,
        )
        print(f"[OK] funcao {FUNCTION_NAME} atualizada")
    except lambda_client.exceptions.ResourceNotFoundException:
        lambda_client.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime="python3.12",
            Role=role_arn,
            Handler="handler.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=180,
            MemorySize=256,
            Environment=env,
            Description="Recria gold.onibus_dia_tipo e gold.atraso_por_linha a partir de silver.*",
        )
        print(f"[OK] funcao {FUNCTION_NAME} criada")

    return lambda_client.get_function(FunctionName=FUNCTION_NAME)["Configuration"]["FunctionArn"]


def ensure_schedule(function_arn):
    rule = events.put_rule(
        Name=RULE_NAME,
        ScheduleExpression=SCHEDULE,
        State="ENABLED",
        Description="Dispara safemoove-refresh-gold periodicamente",
    )
    rule_arn = rule["RuleArn"]

    events.put_targets(
        Rule=RULE_NAME,
        Targets=[{"Id": "refresh-gold-target", "Arn": function_arn}],
    )

    try:
        lambda_client.add_permission(
            FunctionName=FUNCTION_NAME,
            StatementId="allow-eventbridge",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
    except lambda_client.exceptions.ResourceConflictException:
        pass  # permissao ja existe de uma execucao anterior

    print(f"[OK] regra {RULE_NAME} ({SCHEDULE}) apontando pra {FUNCTION_NAME}")


def main():
    print(f"Conta: {ACCOUNT_ID} | Regiao: {REGION}")
    role_arn = ensure_role()
    zip_bytes = build_zip()
    function_arn = ensure_function(role_arn, zip_bytes)
    ensure_schedule(function_arn)
    print("\nDeploy concluido.")
    print(f"Function ARN: {function_arn}")


if __name__ == "__main__":
    main()
