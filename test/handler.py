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
    print(f"Processing CSV - Event: {json.dumps(event)}")
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
            print(f"Processing file: {key} from bucket: {bucket}")
            
            response = s3_client.get_object(Bucket=bucket, Key=key)
            csv_content = response['Body'].read().decode('utf-8')
            print(f"CSV content length: {len(csv_content)}")
            
            conn = psycopg2.connect(
                host=DB_CONFIG['host'],
                database=DB_CONFIG['database'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            print("Database connection established")
            
            cur = conn.cursor()
            
            lines = csv_content.strip().split('\n')
            print(f"Total lines in CSV: {len(lines)}")
            
            # Prepare batch data with deduplication
            seen_emails = set()
            batch_data = []
            duplicates_in_csv = 0
            
            for line in lines[1:]:  # Skip header
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
                    
                    # Check for duplicates within CSV
                    if email in seen_emails:
                        print(f"Skipping duplicate email in CSV: {email}")
                        duplicates_in_csv += 1
                        continue
                    
                    seen_emails.add(email)
                    batch_data.append((user_id, name, email, monthly_income, credit_score, employment_status, age))
                else:
                    print(f"Skipping line with insufficient data: {line}")
            
            print(f"Prepared {len(batch_data)} unique records for batch insert (skipped {duplicates_in_csv} duplicates within CSV)")
            
            # Use ON CONFLICT to ignore duplicates
            insert_query = """
                INSERT INTO users (user_id, name, email, monthly_income, credit_score, employment_status, age) 
                VALUES %s 
                ON CONFLICT (email) DO NOTHING
            """
            
            try:
                from psycopg2.extras import execute_values
                cur.execute("BEGIN")
                execute_values(cur, insert_query, batch_data, template=None, page_size=1000)
                cur.execute("SELECT ROW_COUNT()")
                records_inserted = cur.fetchone()[0] if cur.rowcount > 0 else len(batch_data)
                cur.execute("COMMIT")
                print(f"Batch insert completed. Records processed: {len(batch_data)}")
            except Exception as e:
                cur.execute("ROLLBACK")
                print(f"Batch insert failed, falling back to individual inserts: {str(e)}")
                
                # Fallback to individual inserts
                records_inserted = 0
                for data in batch_data:
                    try:
                        cur.execute(
                            "INSERT INTO users (user_id, name, email, monthly_income, credit_score, employment_status, age) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING",
                            data
                        )
                        if cur.rowcount > 0:
                            records_inserted += 1
                    except Exception as individual_error:
                        print(f"Skipping record {data[0]}: {str(individual_error)}")
                        continue
                
                conn.commit()
            cur.close()
            conn.close()
            print(f"Successfully inserted {records_inserted} records to database")
            
            # Call webhook after processing
            webhook_url = "https://httpbin.org/post"  # Sample webhook - replace with your actual webhook
            webhook_data = {
                "message": "CSV processed successfully",
                "filename": key,
                "processed_at": "now",
                "records_processed": records_inserted
            }
            
            try:
                webhook_response = requests.post(webhook_url, json=webhook_data, timeout=10)
                print(f"Webhook called successfully: {webhook_response.status_code}")
            except Exception as webhook_error:
                print(f"Webhook failed: {webhook_error}")
            
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "CSV processed successfully"})
        }
        
    except Exception as e:
        print(f"Error processing CSV: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
