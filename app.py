from flask import Flask, request, jsonify, send_file, Response
import logging
from flask_cors import CORS
import psycopg2
from dotenv import load_dotenv
import os
import json
import pandas
from datetime import datetime
import csv
from werkzeug.utils import secure_filename

logging.basicConfig(filename='record.log', level=logging.DEBUG)
app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'json'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


CORS(app)

# Load environment variables
load_dotenv()
connection_string = os.getenv("EXTERNAL_DATABASE_URL")


def establish_connection():
    try:
        connection = psycopg2.connect(connection_string)
        return connection
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None


def get_query_result(query, params, get=True):
    connection = establish_connection()
    if not connection:
        return jsonify({"error": "Unable to connect to the database"})

    try:
        cursor = connection.cursor()
        cursor.execute(query, params)
        connection.commit()
        if (get):
            data = cursor.fetchall()
            # print(query,params)
            cursor.close()
            connection.close()
            # print("this is to check date: \n", data, "\n\n")

            # Get column names from cursor.description
            column_names = [desc[0] for desc in cursor.description]

            # Convert the query result to a list of dictionaries with column names as keys
            result = [dict(zip(column_names, row)) for row in data]
            # print((result))
            return (result)
            # return jsonify(result)
        # print(query % params)
        cursor.close()
        connection.close()

    except psycopg2.Error as e:
        print("Error executing SQL query:", e)
        connection.close()
        return jsonify({"error": "Error fetching records"})


count = None


@app.route("/home", methods=['GET'])
def give_books():
    page = request.args.get("page", type=int, default=1)
    per_page = request.args.get("per-page", type=int, default=10)
    offset = (page - 1) * per_page
    query = 'SELECT * FROM books LIMIT %s OFFSET %s;'

    params = (per_page, offset)
    data = get_query_result(query, params, True)

    return data


@app.route('/query/count', methods=['GET'])
def frontend_query_response_count():
    # Start with a true condition
    query = 'SELECT count(*) FROM books WHERE 1=1'

    # Initialize parameters list
    params = []

    start_date = request.args.get("start-date", type=str)
    end_date = request.args.get("end-date", type=str)
    order = request.args.get("order", type=str, default='asc')
    paid = request.args.get("paid")
    genre = request.args.get("genre")

    # Build the query dynamically based on provided parameters
    if start_date:
        query += " AND publisheddate >= '%s-01-01' "
        params.append(int(start_date))
    if end_date:
        query += " AND publisheddate <= '%s-01-01' "
        params.append(int(end_date))
    if paid:
        query += " AND paid = %s"
        params.append(paid)
    if genre:
        query += " AND (title ~* %s OR %s ~* ANY(categories))"
        params.extend([genre, genre])

    data = get_query_result(query, tuple(params), True)
    return data


@app.route('/query', methods=['GET'])
def frontend_query_response():
    query = '''SELECT * FROM books where'''
    params = ()
    start_date = request.args.get("start-date", type=int, default=1900)
    end_date = request.args.get("end-date", type=int, default=2030)
    order = request.args.get("order", type=str, default='asc')
    paid = request.args.get("paid")
    genre = request.args.get("genre")
    page = request.args.get("page", type=int, default=1)
    per_page = request.args.get("per-page", type=int, default=10)
    offset = (page - 1) * per_page

    if (start_date is not None):
        query += " publisheddate > '%s-01-01' "
        params = params + (start_date,)
    if (end_date is not None):
        query += " and publisheddate < '%s-01-01' "
        params = params + (end_date,)
    if (paid is not None):
        query += " and paid= %s"
        params = params + (paid,)
    if (len(genre) > 1):
        query += " and ( title ~* %s or %s ~* ANY(categories))"
        params = params+(genre, genre)
    if (order is not None):
        query += " order by publisheddate "+order

    query += " Limit %s offset %s "
    params = params + (per_page, offset)
    data = get_query_result(query, params, True)
    print("\ncount->", count)
    return data


@app.route('/book/<operation>/<id>', methods=['GET', 'PATCH', 'DELETE', 'POST'])
def book_operations(operation, id):
    if (operation == 'bookDetils'):
        query = 'SELECT * FROM books WHERE _id=%s'
        # query = '''SELECT _id, title ,isbn , pagecount ,
        # publisheddate::text ,
        # thumbnailurl::text ,shortdescription::text ,longdescription ,status ,
        # authors ,categories ,paid , price FROM BOOKS where _id=%s'''
        params = (id,)
        data = get_query_result(query, params, True)
        return data

    # if(request.method == 'PATCH'):
    if (operation == 'update'):
        data = request.get_json()
        title = data.get("title")
        isbn = data.get("isbn")
        pagecount = data.get("pagecount")
        publisheddate = data.get("publisheddate")
        categories = data.get("categories")
        # categories = categories
        longdescription = data.get("longdescription")
        paid = data.get("paid")
        price = data.get("price")
        shortdescription = data.get("shortdescription")
        status = data.get("status")
        thumbnailurl = data.get("thumbnailurl")
        authors = data.get("authors")

        # print(data)

        query = " UPDATE BOOKS SET title = %s , isbn = %s , pagecount= %s , publisheddate= %s , categories= Array[%s] , longdescription= %s , paid= %s , price=%s , shortdescription=%s , status= %s , thumbnailurl=%s, authors=Array[%s] where _id= %s"
        params = (title, isbn, pagecount, publisheddate, categories, longdescription,
                  paid, price, shortdescription, status, thumbnailurl, authors, id)
        try:
            get_query_result(query, params, False)
            return "Successfully updated ", 202
        except Exception as e:
            return str(e), 500

        # print(query % params)
        # return data

    if (request.method == 'DELETE'):
        query = 'DELETE FROM BOOKS WHERE _id=%s'
        params = (id,)
        try:
            get_query_result(query, params, False)
            return "The book was successfully deleted !", 202
        except:
            return "Error Occured !", 500

    if (request.method == 'POST'):
        data = request.get_json()
        title = data.get("title")
        isbn = data.get("isbn")
        pagecount = data.get("pagecount")
        publisheddate = data.get("publisheddate")
        publisheddate = datetime.strptime(
            publisheddate, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d")
        categories = data.get("categories")
        # categories = categories
        longdescription = data.get("longdescription")
        paid = data.get("paid")
        price = data.get("price")
        shortdescription = data.get("shortdescription")
        status = data.get("status")
        thumbnailurl = data.get("thumbnailurl")
        authors = data.get("authors")

        query = """INSERT INTO BOOKS(title, isbn, pagecount , publisheddate , thumbnailurl, shortdescription, longdescription, status, authors, categories, paid, price)
            VALUES (%s, %s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        params = (title, isbn, pagecount, publisheddate, thumbnailurl,
                  shortdescription, longdescription, status, authors, categories, paid, price)

        try:
            get_query_result(query, params, False)
            return "The book was successfully inserted !", 202
        except psycopg2.Error as e:
            return "Error :"+str(e), 500


@app.route('/create', methods=['POST'])
def create_book():
    print("THis is create route and it was hit")
    data = request.get_json()
    print("after req.get_json()")
    print(data.get("title"))
    title = data.get("title")
    isbn = data.get("isbn")
    pagecount = data.get("pagecount")
    publisheddate = data.get("publisheddate")
    # ----------------Made change here-----------------
    # publisheddate = data.get("publisheddate")
    # publisheddate = datetime.strptime(
    #     publisheddate, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d")

    # --------------till here
    categories = list(''.join(data.get("categories")))
    # categories = categories
    longdescription = data.get("longdescription")
    paid = data.get("paid")
    price = data.get("price")
    shortdescription = data.get("shortdescription")
    status = data.get("status")
    thumbnailurl = data.get("thumbnailurl")
    authors = list(','.join(data.get("authors")))
    print(authors, categories)

    print("we came thill before query !")

    query = """INSERT INTO BOOKS(title, isbn, pagecount , publisheddate , thumbnailurl, shortdescription, longdescription, status, authors, categories, paid, price)
        VALUES (%s, %s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    params = (title, isbn, pagecount, publisheddate, thumbnailurl,
              shortdescription, longdescription, status, authors, categories, paid, price)
    try:
        get_query_result(query, params, False)
        return "The book was successfully created !", 201
    except:
        return "Error Occured !", 500


@app.route('/home/count', methods=['GET'])
def book_count():

    query = 'SELECT COUNT(*) FROM BOOKS ;'
    params = ()
    data = get_query_result(query, params)
    return data


@app.route('/search', methods=['GET'])
def search_book():
    print("in search !")
    search_query = request.args.get("search-query")
    page = request.args.get("page", type=int, default=1)
    per_page = request.args.get("per-page", type=int, default=10)
    offset = (page - 1) * per_page
    print(search_query)
    query = """
        SELECT *
    FROM books
    WHERE
        title ILIKE %s
        OR isbn ILIKE %s
        OR pagecount ILIKE %s
        OR publisheddate ILIKE %s
        OR thumbnailurl ILIKE %s
        OR shortdescription ILIKE %s
        OR longdescription ILIKE %s
        OR status ILIKE %s
        OR %s ILIKE any(authors) 
        OR %s ILIKE any(categories)
        OR paid ILIKE %s
        OR price ILIKE %s
        LIMIT %s
        OFFSET %s
    """
    print("after query ")
    params = (search_query, search_query, search_query,
              search_query, search_query, search_query,
              search_query, search_query, search_query,
              search_query, search_query, search_query, per_page, offset)
    print(query % params)
    data = get_query_result(query, params)
    # print(params, data)
    return data


@app.route('/export-json-data', methods=['GET'])
def export_json_data():
    # query = '''SELECT _id, title,isbn, pagecount,
    # TO_CHAR(publisheddate,'DD/MM/YYYY') as publisheddate ,
    # thumbnailurl,shortdescription,longdescription,status,
    #   authors,categories,paid, price::text FROM BOOKS '''
    query = "select row_to_json(row) from (select * from books) row"
    params = ()
    data = get_query_result(query, params, True)
    # print(data, query)
    with open("jsonData.txt", "w") as output_file:
        for record in data:
            # json.dump(record, output_file, indent=2)
            output_file.write(str(record)+"\n")
        output_file.close()

    print("writing successfull!!")
    return send_file("jsonData.txt", as_attachment=True, mimetype='application/json')

    # return data


@app.route('/export-csv-data', methods=['GET'])
def method_name():
    connection = establish_connection()
    cursor = connection.cursor()
    query = '''SELECT _id::text, title ::text ,isbn::text , pagecount::text ,
      publisheddate::text ,
    thumbnailurl::text ,shortdescription::text ,longdescription::text ,status::text ,
      authors::text ,categories::text ,paid::text , price::text FROM BOOKS '''
    # TO_CHAR(publisheddate,'DD/MM/YYYY')
    params = ()
    cursor.execute(query, params)
    data = cursor.fetchall()
    with open("data.csv", "w") as csv_ptr:
        writer = csv.writer(csv_ptr)

        # write the column names
        writer.writerow([col[0] for col in cursor.description])

        # write the query results
        writer.writerows(data)
    cursor.close()
    print("CSV WRITTING SUCCESS")
    connection.close()
    # data = pandas.read_csv("data.csv")
    # data.to_csv("data.csv")
    return send_file("data.csv", as_attachment=True, mimetype='application/json')

    # return data


@app.route('/uplode/<file_type>', methods=['POST'])
def uplode_files(file_type):

    file = request.files['file']
    print(file)

    if file:
        filename = secure_filename(file.filename)
        file.save(os.path.join('./', filename))
        print("File Type:", file_type)
        print(filename)
        if (file_type == 'csv'):
            query = """
            INSERT INTO BOOKS(title, isbn, pagecount , publisheddate , thumbnailurl, shortdescription, longdescription, status, authors, categories, paid, price)
            VALUES (%s, %s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            with open(filename, "r") as file_ptr:
                df = pandas.read_csv(filename)
                for row in range(0, df.shape[0]):
                    # print(df.iloc[row]["isbn"])
                    params = (
                        str(df.iloc[row]['title']), str(df.iloc[row]['isbn']),
                        str(df.iloc[row]['pagecount']), str(
                            df.iloc[row]['publisheddate']),
                        str(df.iloc[row]['thumbnailurl']), str(
                            df.iloc[row]['shortdescription']), str(df.iloc[row]['longdescription']),

                        str(df.iloc[row]['status']), str(df.iloc[row]['authors']), str(
                            df.iloc[row]['categories']), str(df.iloc[row]['paid']),
                        str(df.iloc[row]['price'])
                    )
                    get_query_result(query, params, False)

            os.remove(filename)
            print("File deleted!!")
            return "File deleted successfully !", 202

        else:
            print("Json data recieved!!")
            with open(filename, "r") as json_ptr:
                for row in json_ptr:
                    # Replace single quotes with double quotes
                    row = row.replace("\\'", "`")
                    # Replace single quotes with double quotes
                    row = row.replace("\"", "`")
                    # Replace single quotes with double quotes
                    row = row.replace("'", "\"")
                    data = json.loads(row)
                    print("data become json \n")
                    # print("data\n", data["row_to_json"])
                    data = data['row_to_json']
                    print(data['title'])
                    query = """
                    INSERT INTO BOOKS(title, isbn, pagecount ,
                    publisheddate , thumbnailurl, shortdescription, 
                    longdescription, status, authors, categories, paid, price)
                    VALUES (%s, %s, %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    params = (
                        data['title'], data['isbn'],
                        data['pagecount'], data['publisheddate'],
                        data['thumbnailurl'], data['shortdescription'], data['longdescription'],
                        data['status'], data['authors'], data['categories'], data['paid'],
                        data['price']
                    )
                    print(query % params)

                print("json data successfull added !!")

            os.remove(filename)
            print("File deleted!!")
            return "file recieved ! ", 200


if __name__ == "__main__":
    app.run()
