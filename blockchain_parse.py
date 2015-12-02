import psycopg2
import sys
from blockchain_parser.blockchain import Blockchain
import time


try:
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='test123'")
except:
    print ("I am unable to connect to the database")

def writetoblock(id, height, timestamp):
    print('BLOCK:  ', id, height, timestamp)
    cur = conn.cursor()
    cur.execute("INSERT INTO block (id,height,timestamp) VALUES (%s,%s,%s)", (id, height, timestamp))
    conn.commit()
def writetotransaction(id, block_id, locktime, size, trans_type, transaction_size):
        print('TRANSACTION:  ', id, block_id, locktime, size, trans_type, transaction_size)
        cur = conn.cursor()
        cur.execute("INSERT INTO transaction (id, block_id, locktime, value, type, size) VALUES (%s,%s,%s,%s,%s,%s)", (id, block_id, locktime, size, trans_type, transaction_size))
        conn.commit()
def writetoinput(transaction_id, output_id, number, type, seq_no):
    print('INPUT:  ', transaction_id, output_id, number, type, seq_no)
    cur = conn.cursor()
    cur.execute("INSERT INTO input (transaction_id, output_id, number, type, seq_no) VALUES (%s,%s,%s,%s,%s)", (transaction_id, output_id, number, type, seq_no))
    conn.commit()
def writetooutput(transaction_id, unspent, output_type, value, number, type_meta):
        print('OUTPUT:  ',transaction_id, unspent, str(output_type), value, number, str(type_meta))
        cur = conn.cursor()
        cur.execute("INSERT INTO output (transaction_id, unspent, type, value, number, type_meta) VALUES (%s,%s,%s,%s,%s,%s)", (transaction_id, unspent, output_type, value, number, type_meta))
        conn.commit()
def writetop2sh(input_id, full_script):
    print('P2SH:  ', input_id, full_script)
    cur = conn.cursor()        
    cur.execute("INSERT INTO p2sh (input_id, full_script) VALUES (%s,%s)", (input_id, full_script))
    conn.commit()
def writetooutput_script(output_id, full_script):
        print('OUTPUT_SCRIPT:  ', output_id, full_script)
        cur = conn.cursor()        
        cur.execute("INSERT INTO output_script (output_id, full_script) VALUES (%s,%s)", (output_id, full_script))
        conn.commit()
def fetch_single_data(table, column, id):
    cur.execute("SELECT %s FROM %s WHERE id = %s", (column, table, id))
    return cur.fetchone()


# Instanciate the Blockchain by giving the path to the directory 
blockchain = Blockchain(sys.argv[1])
print(blockchain.get_main_chain())
#ordere blocks according to height starting with genesis block
#orderedblockchain= blockchain.get_main_chain()
#print(type(orderedblockchain))
#print(orderedblockchain[0])
#get relevant data from relevant blocks and save to db
def saveblockinfo(block):   
    height = block.height
    id = block.hash
    header = block.header
    timestamp = time.mktime(header.timestamp.timetuple())
    #writetoblock(id, height, timestamp)
    writetoblock(id, height, timestamp)

#returns type of transaction
#types are: coinbase, paytopubkey, paytopubkeyhash, p2sh, op_return
def get_transaction_type(transaction):
    type = ''
    for input in transaction.inputs:
        if input.transaction_hash == "0" * 64:
            type = 'coinbase'
            return type
    for output in transaction.outputs:
        if type == '':
            outputtype = output.type
            if outputtype != 'unknown':
                type = outputtype
        else:
            if outputtype != 'unknown':
                type += outputtype
            
    return type



#gets all transactions in block, the relevant data
#tries to write it to database    
def savetransactions(block):
    for trans in block.transactions:
        transid = trans.hash
        block_id = block.hash
        locktime =  trans.locktime
        value = 0
        for output in trans.outputs:
            value += output.value
        size = trans.size
        for input in trans.inputs:
            hash = input.transaction_hash
            index = input.transaction_index
        trans_type = get_transaction_type(trans)
        writetotransaction(transid, block_id, locktime, value, trans_type, size)
        saveoutput(trans)
        saveinput(trans)

def saveoutput(transaction):
    cur = conn.cursor()
    number = 0
    for output in transaction.outputs:
        trans_id = transaction.hash
        unspent = True
        type = output.type
        value = output.value
        type_meta = ''
        addresses = output.addresses
        if addresses != []:
            if len(addresses)==1:
                type_meta = addresses[0].address
                print(type_meta)
            else:
                for address in addresses:
                    newlist = []
                    newlist.append(address.address)
                addresses = newlist
                type_meta = ', '.join(addresses)
        if type == 'OP_RETURN':
            try:
                type_meta = output.script.value.split('OP_RETURN ')[1]
            except:
                pass
        writetooutput(trans_id, unspent, type, value, number, str(type_meta))
        cur.execute("SELECT id FROM output WHERE transaction_id = %s AND number = %s", (trans_id, number))
        output_id = int(cur.fetchone()[0])
        writetooutput_script(output_id, output.script.value)
        number += 1

def saveinput(transaction):
    number = 0
    cur = conn.cursor()
    inputvalue = 0
    type = ''
    for input in transaction.inputs:
        transaction_id = transaction.hash
        reedemed_output_trans_id = input.transaction_hash
        output_number = input.transaction_index
        if output_number != 4294967295:
            cur.execute("SELECT * FROM output WHERE transaction_id = %s AND number = %s", (reedemed_output_trans_id, output_number))
            unspent, type, output_trans_id, value, output_number, type_meta, output_id = cur.fetchone()
            inputvalue += value
            cur.execute("UPDATE output SET unspent = %s WHERE transaction_id = %s AND number =%s", (False, reedemed_output_trans_id, output_number))
            if type == 'p2sh':
                cur.execute("SELECT id FROM input WHERE transaction_id = %s AND output_id = %s", (transaction_id, output_id))
                input_id = int(cur.fetchone()[0])
                full_script = input.script.value
                writetop2sh(input_id, full_script)        
        seq_no = input.sequence_number

        if output_number != 4294967295:
            cur.execute("SELECT id FROM output WHERE transaction_id = %s AND number = %s", (reedemed_output_trans_id, output_number))
            output_id = int(cur.fetchone()[0])
        else:
            output_id = None    
        if output_number == 4294967295:
            output_number = None
        writetoinput(transaction_id, output_id, number, type, seq_no)

        conn.commit()

    cur.execute("SELECT value FROM transaction WHERE id = %s", (transaction_id,))
    output_value = int(cur.fetchone()[0])
    fee = output_value - inputvalue
    if output_number == None:
        fee = 0
    cur.execute("UPDATE transaction SET fee = %s WHERE id = %s ", (fee, transaction_id))
    conn.commit()


blockchain = Blockchain(sys.argv[1])
counter = 0
for block in blockchain.get_unordered_blocks():
    saveblockinfo(block)
    savetransactions(block)
    print('___')
    print (counter)
    counter +=1

try:
    cur.close()
except:
    print('no cursor to close')
try:
    conn.close()
except:
    print('no connection to close')
