import json
import boto3
import psycopg2
import requests
from botocore.exceptions import ClientError


def get_presigned_url(event, context):
    s3_client = boto3.client('s3')
    bucket_name = 'loan-user-uploads'
    
    try:
        file_key = f"uploads/{event['queryStringParameters']['filename']}"
        
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket_name, 'Key': file_key},
            ExpiresIn=3600
        )
        
        response = {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
            },
            "body": json.dumps({
                "presignedUrl": presigned_url,
                "key": file_key
            })
        }
        
        return response
        
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }


def process_csv(event, context):
    s3_client = boto3.client('s3')
    
    # Hardcoded database configuration
    DB_CONFIG = {
        'host': 'database-2.crkqi8w6czql.ap-south-1.rds.amazonaws.com',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'Masoom34'
    }
    
    try:
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            response = s3_client.get_object(Bucket=bucket, Key=key)
            csv_content = response['Body'].read().decode('utf-8')
            
            conn = psycopg2.connect(
                host=DB_CONFIG['host'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            
            cur = conn.cursor()
            
            lines = csv_content.strip().split('\n')
            for line in lines[1:]:
                values = line.split(',')
                if len(values) >= 7:
                    user_id = values[0].strip()
                    name = values[1].strip()
                    email = values[2].strip()
                    monthly_income = values[3].strip()
                    credit_score = values[4].strip()
                    employment_status = values[5].strip()
                    age = values[6].strip()
                    
                    # Transform employment status
                    if employment_status == 'Self-Employed':
                        employment_status = 'self_employed'
                    elif employment_status == 'Business':
                        employment_status = 'self_employed'
                    elif employment_status == 'Salaried':
                        employment_status = 'salaried'
                    
                    cur.execute(
                        "INSERT INTO users (user_id, name, email, monthly_income, credit_score, employment_status, age, filename, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (user_id, name, email, monthly_income, credit_score, employment_status, age, key, 'processed')
                    )
            
            conn.commit()
            cur.close()
            conn.close()
            
            # Call webhook after processing
            webhook_url = "https://httpbin.org/post"  # Sample webhook - replace with your actual webhook
            webhook_data = {
                "message": "CSV processed successfully",
                "filename": key,
                "processed_at": "now",
                "records_processed": len(lines) - 1
            }
            
            try:
                requests.post(webhook_url, json=webhook_data, timeout=10)
            except:
                pass  # Don't fail the Lambda if webhook fails
            
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "CSV processed successfully"})
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
