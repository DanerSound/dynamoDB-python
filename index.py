from boto3 import resource
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime

demo_table = resource('dynamodb').Table('demo-dynamo-python')

def insert():
    print(f'demo-insert')
    response = demo_table.put_item(
        Item={
            'customer_id':'cus-099', # partition key (primary key-mandatory)
            'order_id':'ord-41', # sort key (mandatory)
            'status':'pending',
            'create_date': datetime.now().isoformat()
        }
    )
    print(f' Insert response: {response}')

def select_scan():
    print(f'demo_select_scan')
    filter_expression = Attr('status').eq('pending')

    item_list = []
    dynamo_response = {'LastEvaluatedKey':False}
    while 'LastEvaluatedKey' in dynamo_response:
        if dynamo_response['LastEvaluatedKey']:
            dynamo_response=demo_table.scan( # cost too much! 
                FilterExpression=filter_expression,
                ExclusiveStartKey=dynamo_response['LastEvaluatedKey']
                )
            print(f'response-if:{dynamo_response}')
        else:
            dynamo_response = demo_table.scan(
                FilterExpression=filter_expression,
            )
            print(f'response-else{dynamo_response}')

        for item in dynamo_response['Items']:
            item_list.append(item)

    print(f'Number of input task to process: {len(item_list)}')
    for item in item_list:
        print(f'Item: {item}')

def query_by_partition_key(customer_value):
    print("query by customer id")
    response = {}
    filtering_exp = Key('customer_id').eq(customer_value) # mandatory with are filtering by the given partial key 
    response = demo_table.query(KeyConditionExpression=filtering_exp)

    # print(f'Query response: {response}')
    # print(f'Query response Items: {response["Items"]}')
    item_list = response["Items"]
    for item in item_list:
        print(f'Item:{item}')

def query_by_partition_key_order(customer_value):
    print("query by partion key order")
    response = {}
    filtering_exp = Key('customer_id').eq(customer_value) # mandatory with are filtering by the given partial key 
    response = demo_table.query(
        KeyConditionExpression=filtering_exp,
        ScanIndexForward=True )  # default is False (descending)
    
    item_list = response["Items"]
    for item in item_list:
        print(f'Item:{item}')

# if you need to search by a attributo that is not a key, you can use scan
# but scan cost to much because it will read all the table, solution 
# is using index ( Global and local )
    # global index can be created on the aws dynamo console, 
    # the local index can be created when you create the table

def query_by_index_key(status_value):
    print("demo query index_key")
    filtering_exp = Key('status').eq(status_value) # mandatory with are filtering by the given partial key 
    response = demo_table.query(
        IndexName="status-index",
        KeyConditionExpression=filtering_exp,
        ScanIndexForward=False )  # default is False (descending)
    
    item_list = response["Items"]
    for item in item_list:
        print(f'Item:{item}')

def query_by_partion_key_and_sort_key(customer_value, order_value):
    print ("query by partition + sort keys ")
    response = {}
    filtering_exp = Key('customer_id').eq(customer_value)
    filtering_exp2 = Key('order_id').eq(order_value)
    response = demo_table.query( 
        KeyConditionExpression=filtering_exp & filtering_exp2) # if you use "and" it will break the code 
    
    for item in response["Items"]:
        print (f'Item:{item}')

def update_values(customer_value, status_value): ## we use a reserved key , named status we cant do that
    print ('update operation')
    response = demo_table.update_item(
            Key = {
                'customer_id': customer_value,
            },
            UpdateExpression = 'set status=:r, updated_date=:d',
            ExpressionAttributeValues = {
                ':r': status_value,
                ':d': datetime.now().isoformat()
            },
            
        ReturnValues="UPDATED_NEW"
    )

def update_with_expression_name(customer_value, status_value): ## we use a reserved key , named status we cant do that
    print ('update operation update_with_expression_name')
    response = demo_table.update_item(
            Key = {
                'customer_id': customer_value,
                'order_id':'ord-3'
            },
            UpdateExpression = 'set #status=:r, updated_date=:d', ## this is a placeholder
            ExpressionAttributeValues = {
                ':r': status_value,
                ':d': datetime.now().isoformat()
            },
            ExpressionAttributeNames={
                '#status': 'status'
            },
        ReturnValues="UPDATED_NEW"
    )

    print(f'Response: {response}')

def batch_delete_transation_records(items_to_delete):
    print(f'Deleting transations')
    response = {}
    with demo_table.batch_writer() as batch:
        for item in items_to_delete:
            response = batch.delete_item(
                Key={
                    "customer_id": item["id"],
                    "order_id": item["order_id"]
                }
            )


items = [{"id":"cus-08", "order_id":"order-04"},{"id":"cus-02", "order_id":"order-03"}]
batch_delete_transation_records(items)
# update_with_expression_name('cus-02','completed')
# query_by_partion_key_and_sort_key('cus-02','ord-01')
# query_by_index_key('complete')
# query_by_partition_key_order('cus-02')
# query_by_partition_key('cus-02')
# select_scan()
# insert()