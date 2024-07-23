import boto3
import time


def lambda_handler(event, context):
    # Create Glue client
    glue = boto3.client('glue')

    # Name of the Glue crawler and job to trigger
    crawler_name = 'ecommerce-data-crawler'
    job_name = 'ecommerce-data-transformation'

    # Trigger the Glue crawler
    try:
        response = glue.start_crawler(Name=crawler_name)
        print(f'Started crawler: {response}')
    except Exception as e:
        print(f'Failed to start crawler: {e}')
        return {
            'statusCode': 500,
            'body': f'Failed to start crawler: {e}'
        }

    # Wait for the crawler to complete
    while True:
        try:
            crawler_status = glue.get_crawler(Name=crawler_name)['Crawler']['State']
            if crawler_status == 'RUNNING':
                print('Crawler is still running...')
                time.sleep(60)  # Wait for 1 minute before checking again
            else:
                print(f'Crawler status: {crawler_status}')
                break
        except Exception as e:
            print(f'Error checking crawler status: {e}')
            return {
                'statusCode': 500,
                'body': f'Error checking crawler status: {e}'
            }

    # If the crawler has completed successfully, trigger the Glue job
    if crawler_status == 'READY':
        try:
            print(f'Triggering Glue job: {job_name}')
            job_response = glue.start_job_run(JobName=job_name)
            print(f'Glue job triggered successfully: {job_response}')
        except Exception as e:
            print(f'Failed to trigger Glue job: {e}')
            return {
                'statusCode': 500,
                'body': f'Failed to trigger Glue job: {e}'
            }
    else:
        print('Crawler did not complete successfully. Aborting Glue job trigger.')
        return {
            'statusCode': 500,
            'body': 'Crawler did not complete successfully. Aborting Glue job trigger.'
        }

    # Return a success message
    return {
        'statusCode': 200,
        'body': 'Glue crawler and job triggered successfully'
    }
