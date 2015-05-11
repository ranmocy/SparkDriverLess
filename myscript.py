f = context.text_file('myfile').flat_map(lambda line: line.split()).filter(lambda item: item.isalpha()).map(lambda item: (item, 1)).reduce_by_key(lambda result, item: result + item)
print f.collect()
