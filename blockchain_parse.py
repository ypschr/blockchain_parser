import psycopg2
import sys
from blockchain_parser.blockchain import Blockchain


try:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='test123'")
except:
    print ("I am unable to connect to the database")

def writetoblock(id, height, timestamp):
	cur = conn.cursor()
	cur.execute("INSERT INTO block (id,height,timestamp) VALUES (%s,%s,%s)", (id, height, timestamp))
	conn.commit()
def writetotransaction(id, block_id, locktime, size, type, fee, transaction_size):
        cur = conn.cursor()
        cur.execute("INSERT INTO transaction (id, block_id, locktime, size, type, fee, transaction_size) VALUES (%s,%s,%s,%s,%s,%s,%s)", (id, block_id, locktime, size, type, fee, transaction_size))
        conn.commit()
def writetoinput(transaction_id, output_id, number, type):
        cur = conn.cursor()
        cur.execute("INSERT INTO input (transaction_id, output_id, number, type) VALUES (%s,%s,%s,%s,%s)", (transaction_id, output_id, number, type))
        conn.commit()
def writetooutput(transaction_id, unspent, type, value, number, type_meta):
        cur = conn.cursor()
        cur.execute("INSERT INTO output (transaction_id, unspent, type, value, number, type_meta) VALUES (%s,%s,%s,%s,%s,%s,%s)", (transaction_id, unspent, type, value, number, type_meta))
        conn.commit()
def writetop2sh(input_id, type, full_script):
	cur = conn.cursor()        
	cur.execute("INSERT INTO p2sh (input_id, type, full_script) VALUES (%s,%s,%s)", (input_id, type, full_script))
	conn.commit()
def writetooutput_script(output_id, type, full_script):
        cur = conn.cursor()        
        cur.execute("INSERT INTO output_script (output_id, type, full_script) VALUES (%s,%s,%s)", (output_id, type, full_script))
        conn.commit()


# Instanciate the Blockchain by giving the path to the directory 
blockchain = Blockchain(sys.argv[1])
print(blockchain.get_main_chain())
#ordere blocks according to height starting with genesis block
#orderedblockchain= blockchain.get_main_chain()
#print(type(orderedblockchain))
#print(orderedblockchain[0])
#get relevant data from relevant blocks and save to db
def writeblockinfo(block):	
	height = block.height
	id = block.raw_hex
	header = block.header()
	timestamp = currentheader.timestamp
	#writetoblock(id, currentheight, currenttimestamp)
		
print(test)


#cur.close()
conn.close()


