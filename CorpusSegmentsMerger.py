from azure.storage.table import TableService, Entity

storage_acc_name = 'cfvtes9c07'
storage_acc_key = 'DSTJn6a1dS9aaoJuuw6ZOsnrsiW9V1jODJyHtekkYkc3BWofGVQjS6/ICWO7v51VUpTHSoiZXVvDI66uqTnOJQ=='

vid_id = "test"

service = TableService(account_name=storage_acc_name, account_key=storage_acc_key)
service = TableService(account_name=storage_acc_name, account_key=storage_acc_key)
terms = service.query_entities(table_name='VideosInvertedIndexes',
                                   filter='PartitionKey eq \'' + vid_id + '\'',
                                   select='*')

time_term_dic = {}
if not terms.items:
    raise Exception('No terms for Video ID {} '.format(vid_id))


for record in terms.items:
    current_term = str(record['RowKey'])
    for column in record:
        if column.startswith("t_"):
            for char in ["t",'_'] : column = column.replace(char, "")
            time_term_dic[int(column)] = current_term

sorted_time_term_dic = {k: time_term_dic[k] for k in sorted(time_term_dic)}

transcript = " ".join(sorted_time_term_dic.values())
print (transcript)