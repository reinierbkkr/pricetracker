'''
Added in this version:
added when no price found it will check if this happened before
added column to product table with status (no price found), status is registered
after two times or more no price found
when status for product 'no price' it will be excluded in updateallprices

next version:
add documentation
make funtion to detect discount
'''

import requests, bs4, sqlite3, traceback, os, sys, time
from datetime import datetime, timedelta

version = '1.0.5'

date = datetime.today().strftime('%Y%m%d')

pathubu = '/home/reinier/env/'
pathwin = 'C:/Pricetracker/'

if os.path.exists(pathubu):
    path = pathubu
    opsys = 'Ubuntu'
else:
    path = pathwin
    opsys = 'Windows'
    

def logstartsession(version, writetodb, min):
    string = f"\n{datetime.now()}: pricetracker {version} session started on \
{opsys} with writetodb = {writetodb} and min = {min}\n"
    with open(f'{path}/log/{date}log.txt', 'a') as f:
        f.write(string)
    with open(f'{path}/log/{date}logprint.txt', 'a') as f:
        f.write(string)

def log(func): #decorator
    def inner(*args, **kwargs):
        with open(f'{path}/log/{date}log.txt', 'a') as f:
            f.write(f"{datetime.now()}:\n\
calling '{func.__name__}', with args: {args}, with kwargs: {kwargs} \n")
            try:
                return func(*args, **kwargs)
            except Exception as e:
                f.write(f'Error: {e}\n{traceback.format_exc()}')
                raise
    return inner

def logprint(*args):
    with open(f'{path}/log/{date}logprint.txt', 'a') as f:
        f.write(f"{datetime.now()}: " + " ".join([arg for arg in args]) + '\n')
    print(f"{datetime.now()}: " + " ".join([arg for arg in args]))


class DatabaseManager(object):
    @log
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.conn.execute('pragma foreign_keys = on')
        self.conn.commit()
        self.cur = self.conn.cursor()

    @log
    def execute(self, *args):
        if len(args) <= 2:
            self.cur.execute(*args)
            self.conn.commit()
            return self.cur
        else:
            raise TypeError('More than 2 arguments given')

    @log
    def query(self, *args):
        if len(args) <= 2:
            self.cur.execute(*args)
            #self.conn.commit() # I removed this because not necessary
            return self.cur.fetchall()
        else:
            raise TypeError('More than 2 arguments given')

    @log
    def __del__(self):
        self.conn.close()


class Product: # init en getprice zijn uniek per shop, maak subclasses voor andere shops
    db = f'{path}database.db'

    @log
    def __init__(self, id=None, url=None):
        self.__storeid = 1
        
        # can still make object when Product(1) or Product(None, 'http://url.com/')
        if not id == None and not url == None:
            raise ValueError('Expected id or url to be given, not both')

        elif id != None:
            self.__id = id
            try:
                self.__name, self.__ean, self.__url = \
                    self.fetchdetails()
            except IndexError:
                logprint('failed to produce Product object')
            except TypeError:
                logprint('failed to produce Product object')

        elif url != None:
            self.__url = url

            page = self.fetchpage()

            self.__ean = 'not found'
            specs = page.select('div[class="specs__row"]')
            for spec in specs:
                spec = spec.getText().split()
                if ('EAN' in spec):
                    self.__ean = spec[1]
                    break

            try:
                self.__name = page.select('span[class="u-mr--xs"]')[0].getText()
            except IndexError as e:
                logprint(f'error finding name: {e}')
                self.__name = 'not found'

            self.writetodb()

            self.__id = self.fetchid()

            logprint('object made from url')

    @log
    def writetodb(self):
        logprint(f"writing product '{self.__name}' to database")
        db = DatabaseManager(Product.db)
        db.execute('''
        INSERT INTO products (name, ean, url, store_id)
        VALUES (?, ?, ?, ?);''',
                   (self.__name, self.__ean, self.__url, self.__storeid))

    @log
    def fetchid(self):
        db = DatabaseManager(Product.db)
        id = db.query('''
        SELECT product_id
        FROM products
        WHERE url = (?)
        LIMIT 1;''',
                   (self.__url,))[0][0]
        return id

    @log
    def fetchdetails(self):
        db = DatabaseManager(Product.db)
        try:
            name, ean, url = db.query('''
            SELECT name, ean, url
            FROM products
            WHERE product_id = ?;''',
                   (self.__id,))[0]
            return name, ean, url
        except IndexError:
            logprint(f'no product with id {self.__id} found')
            raise

    @log
    def fetchpage(self):
        try:
            res = requests.get(self.__url)
            res.raise_for_status()
            #len(res.content)) #maybe add size check to prevent memory overload
            page = bs4.BeautifulSoup(res.text, 'lxml')
        except requests.exceptions.HTTPError as e:
            raise
        return page

    @log
    def getprice(self):
        page = self.fetchpage()
        try:
            price, pricefr = page.select('span[class="promo-price"]')[0].getText().split()
            if pricefr == '-': pricefr = 0
            price = float(f'{price}.{pricefr}')
            return price
        except IndexError as e: #will be raised when no span with this tag is found
            logprint(f'error finding price: {e}')
            raise
        except UnboundLocalError as e:
            logprint(f'error finding price: {e}')
            raise

    @log
    def updateprice(self, writetodb=True):
        try:
            price = self.getprice()

            logprint(f"writing current price '{price}' for product id {self.__id} to database.")
            
            if writetodb:
                db = DatabaseManager(Product.db)
                db.execute('''
                INSERT INTO product_prices (product_id, price_date, price, store_id)
                VALUES (?, DATETIME('now','localtime'), ?, 1);''',
                       (self.__id, price))
        except IndexError as e:
            logprint(f'no price registered for product with id {self.__id}')
            self.writestatustodb('no price', writetodb=writetodb)
            if self.checkrepeatstatus() >= 2:
                self.commitproductstatus('no price', writetodb=writetodb)
                
        except UnboundLocalError as e: #don't know when this will be raised
            logprint(f'no price registered for product with id {self.__id}')
            self.writestatustodb('no price, unboundlocalerror',
                                 writetodb=writetodb)
            if self.checkrepeatstatus() >= 2:
                self.commitproductstatus('no price', writetodb=writetodb)           
    @log
    def writestatustodb(self, status, writetodb=True):
        if writetodb:
            db = DatabaseManager(Product.db)
            db.execute('''
            INSERT INTO product_prices (product_id, price_date, store_id, status)
            VALUES (?, DATETIME('now','localtime'), 1, ?);''',
                   (self.__id, status))     
    
    @log
    def checkrepeatstatus(self):
        db = DatabaseManager(Product.db)
        repeats = db.query('''
        SELECT COUNT(status)
        FROM product_prices
        WHERE product_id = ?
        AND status = 'no price';''',(self.__id,))[0][0]
        return repeats
    
    @log
    def commitproductstatus(self, status, writetodb=True):
        if writetodb:
            logprint(f'writing status {status} to product with id {self.__id}')
            db = DatabaseManager(Product.db)
            db.execute('''
            UPDATE products
            SET status = ?
            WHERE product_id = ?;''', (status, self.__id))
           
           
    @log
    def deletefromdb(self):
        logprint(f"{datetime.now()}: deleting product with id '{self.__id}' from database.")
        db = DatabaseManager(Product.db)
        db.execute('''
        DELETE FROM product_prices
        WHERE product_id = ?;''', (self.__id,))
        db.execute('''
        DELETE FROM products
        WHERE product_id = ?;''', (self.__id,))

    def getid(self):
        return self.__id

    def geturl(self):
        return self.__url

    def getean(self):
        return self.__ean

    def getname(self):
        return self.__name


### I guess this should be a db class:

@log
def addproductstodb(string):
    url = f'https://www.bol.com/nl/nl/s/?searchtext={string}'
    try:
        res = requests.get(url)
        res.raise_for_status()
        #len(res.content)) #maybe add size check to prevent memory overload
        page = bs4.BeautifulSoup(res.text, 'lxml')
    except requests.exceptions.HTTPError as e:
        logprint('Url is not working:', e.response.status_code)
        raise 
    try:
        links = page.select('a[role="heading"]')
        urls = []
        for link in links:
            link  = link['href']
            urls.append(f'https://bol.com{link}')
    except IndexError:
        logprint(f'no product found for ean: {ean}')

    if input(f'found {len(urls)} urls, \
    want to add all to database? y/n?\n').lower() == 'y':
        for url in urls:
            p = Product(url=url)
            p.updateprice()
    logprint('done')

@log
def removeduplicates():
    db = DatabaseManager(Product.db)
    products = db.query('''
    SELECT product_id, ean
    FROM products;''',)

    idduplicates = []
    eans = []
    for product in products:
        if product[1] in eans:
            idduplicates.append(product[0])
        else:
            eans.append(product[1])
    logprint(f'found {len(idduplicates)} duplicate entries')

    for id in idduplicates:
        p = Product(id=id)
        p.deletefromdb()

@log
def removefaultyentries():
    db = DatabaseManager(Product.db)
    products = db.query('''
    SELECT product_id, name, ean, url
    FROM products;''',)

    idfaulty = []
    for product in products:
        if product[1] == 'not found' or \
           product[2] == 'not found' or \
           product[3] == None:
            idfaulty.append(product[0])
    logprint(f'found {len(idfaulty)} faulty entries')

    for id in idfaulty:
        p = Product(id=id[0])
        p.deletefromdb()

@log
def getallids():
    db = DatabaseManager(Product.db)
    idstuples = db.query('''
    SELECT product_id
    FROM products
    WHERE status IS NULL;''',) #get all ids where no status registered
    ids=[]
    for id in idstuples:
        ids.append(id[0])
    return ids

## old version
#@log
#def updateallprices(min=0,writetodb=True):
    #'''
    #requests.exceptions.HTTPError: 429 Client Error: Too Many Requests.
    #probably rate limiting function of store server. add way to circumvent this
    #in future when mass data is requested.
    #also ran into 503 (service_unavailable). Add way to request these later.
    #'''
    #allids = getallids()
    #todoids = []
    #for id in allids:
        #if id >= min:
            #todoids.append(id)

    #for i in range(0,len(todoids),100): #range 0-150 steps 100
        #for id in todoids[i:i+100]:
            #try:
                #p = Product(id=id)
                #p.updateprice(writetodb=writetodb)
            #except requests.exceptions.HTTPError as e:
                #code = e.response.status_code
                #logprint(f'Error finding price for id {id}: {e}: \
                #{e.response.status_code}')
                #if code in (529, 503):
                    #trylater = 'yes'
                #else:
                    #p.writestatustodb(f'HTTPError: {code}')
            #except Exception as e:
                #logprint(f'unknown error: {e}')
                #break
        #if len(todoids[i:i+100]) == 100:
            #logprint('One minute time-out')
            #time.sleep(60)

##this one should work
@log
def updateallprices(min=0,writetodb=True):
    '''
    requests.exceptions.HTTPError: 429 Client Error: Too Many Requests.
    probably rate limiting function of store server. add way to circumvent this
    in future when mass data is requested.
    also ran into 503 (service_unavailable). Add way to request these later.
    '''
    allids = getallids()
    todoids = []
    for id in allids:
        if id >= min:
            todoids.append(id)
    
    tryagain = []

    for i in range(0,len(todoids),100): #range 0-150 steps 100
        for id in todoids[i:i+100]:
            try:
                p = Product(id=id)
                p.updateprice(writetodb=writetodb)
            except requests.exceptions.HTTPError as e:
                code = e.response.status_code
                logprint(f'Error finding price for id {id}: {e}: \
                {code}')
                if code in (429, 503):
                    tryagain.append(id)
                else:
                    p.writestatustodb(f'HTTPError: {code}', writetodb=writetodb)                
            except Exception as e:
                logprint(f'unknown error: {e}')
                break
        if len(todoids[i:i+100]) == 100:
            logprint('One minute time-out')
            time.sleep(60)
            
    if tryagain:
        for id in tryagain:
            try: #make separate function of this
                p = Product(id=id)
                p.updateprice(writetodb=writetodb)
            except requests.exceptions.HTTPError as e:
                code = e.response.status_code
                logprint(f'Error finding price for id {id}: {e}: \
                {code}')
                p.writestatustodb(f'HTTPError: {code}', writetodb=writetodb)
            except Exception as e:
                logprint(f'unknown error: {e}')
                break
            
            
def main():
       
    go = True
    min = 0
    writetodb = True
    
    if 0 < len(sys.argv) < 4:
        for arg in sys.argv[1:]:
            if arg[0:4] == 'min=':
                try:
                    min = int(arg[4:])
                except ValueError:
                    logprint('Error detecting value for min. example: min=66')
                    go = False
            elif arg[0:10] == 'writetodb=':
                if arg[10:] == 'True':
                    writetodb = True
                elif arg[10:] == 'False':
                    writetodb = False
                else:
                    logprint('Error detecting value for writetodb. example: writetodb=False')
                    go = False
            else:
                logprint('Error: one or more arguments are invalid')
                go = False
                break
    else:
        logprint('Error: too many arguments given')
        go = False
    
    
    #min = 144 ###CHANGE
    #writetodb = False ###CHANGE
    
    if go:
        logstartsession(version, writetodb, min)
        x = datetime.now()
        updateallprices(min=min, writetodb=writetodb)
        logprint(f'Done. Time elapsed {datetime.now()-x}\n')
    else:
        logprint('Script terminated')


if __name__ == '__main__':
    
    main()    
    

    
