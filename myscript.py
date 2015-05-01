f = context.textFile('myfile').map(lambda s: s.split()).filter(lambda a: int(a[1]) > 2)
print f.collect()
