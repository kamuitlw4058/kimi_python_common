import pybloomfilter
base_bf = pybloomfilter.BloomFilter(10000000, 0.1,'/tmp/new.bloom')


fruit3 = base_bf.copy_template('/tmp/three')
fruit4 = base_bf.copy_template('/tmp/four')
fruit = base_bf.copy_template('/tmp/one')
fruit.update(('apple', 'pear', 'orange', 'apple'))
print(dir(fruit))
print(len(fruit))
#print(fruit.bit_array)
print(fruit.capacity)
print('mike' in fruit)
print('apple' in fruit)
print(fruit.bit_count)
#print(fruit.__dict__)
print(fruit.error_rate)
print(fruit.num_hashes)
print(fruit.num_bits)
print(fruit.num_bits / ( 8 * 1024))
print(fruit.approx_len)
#print(fruit.name)
# print(fruit.to_base64())


fruit2 = base_bf.copy_template('/tmp/two')
fruit2.update(('apple1', 'pear1', 'orange1', 'apple1'))

fruit4 = fruit2.intersection(fruit)
print(f'fruit.approx_len:{fruit4.approx_len}' )

fruit3 =  fruit2.union(fruit)
print(f'fruit.approx_len:{fruit3.approx_len}' )

