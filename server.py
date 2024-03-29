#!/usr/bin/env python2.7

"""
Columbia's COMS W4111.001 Introduction to Databases
Example Webserver

To run locally:

    python server.py

Go to http://localhost:8111 in your browser.

A debugger such as "pdb" may be helpful for debugging.
Read about it online.
"""

import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath('__file__')), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)


#
# The following is a dummy URI that does not connect to a valid database. You will need to modify it to connect to your Part 2 database in order to use the data.
#
# XXX: The URI should be in the format of:
#
#     postgresql://USER:PASSWORD@w4111a.eastus.cloudapp.azure.com/proj1part2
#
# For example, if you had username gravano and password foobar, then the following line would be:
#
#     DATABASEURI = "postgresql://gravano:foobar@w4111a.eastus.cloudapp.azure.com/proj1part2"
#
DATABASEURI = "postgresql://cc3701:xxxxxxxx@w4111vm.eastus.cloudapp.azure.com/w4111"


#
# This line creates a database engine that knows how to connect to the URI above.
#
engine = create_engine(DATABASEURI)

#
# Example of running queries in your database
# Note that this will probably not work if you already have a table named 'test' in your database, containing meaningful data. This is only an example showing you how to run queries in your database using SQLAlchemy.
#


# engine.execute("""CREATE TABLE IF NOT EXISTS test (
#   id serial,
#   name text
# );""")

@app.before_request
def before_request():
  """
  This function is run at the beginning of every web request
  (every time you enter an address in the web browser).
  We use it to setup a database connection that can be used throughout the request.

  The variable g is globally accessible.
  """
  try:
    g.conn = engine.connect()
  except:
    print "uh oh, problem connecting to database"
    import traceback; traceback.print_exc()
    g.conn = None

@app.teardown_request
def teardown_request(exception):
  """
  At the end of the web request, this makes sure to close the database connection.
  If you don't, the database could run out of memory!
  """
  try:
    g.conn.close()
  except Exception as e:
    pass


#
# @app.route is a decorator around index() that means:
#   run index() whenever the user tries to access the "/" path using a GET request
#
# If you wanted the user to go to, for example, localhost:8111/foobar/ with POST or GET then you could use:
#
#       @app.route("/foobar/", methods=["POST", "GET"])
#
# PROTIP: (the trailing / in the path is important)
#
# see for routing: http://flask.pocoo.org/docs/0.10/quickstart/#routing
# see for decorators: http://simeonfranklin.com/blog/2012/jul/1/python-decorators-in-12-steps/
#
@app.route('/')
def index():
  """
  request is a special object that Flask provides to access web request information:

  request.method:   "GET" or "POST"
  request.form:     if the browser submitted a form, this contains the data in the form
  request.args:     dictionary of URL arguments, e.g., {a:1, b:2} for http://localhost?a=1&b=2

  See its API: http://flask.pocoo.org/docs/0.10/api/#incoming-request-data
  """

  # DEBUG: this is debugging code to see what request looks like
  print request.args

  # cursor = g.conn.execute("SELECT name FROM test")
  # names = []
  # for result in cursor:
  #   names.append(result['name'])  # can also be accessed using result[0]
  # cursor.close()

  #
  # Flask uses Jinja templates, which is an extension to HTML where you can
  # pass data to a template and dynamically generate HTML based on the data
  # (you can think of it as simple PHP)
  # documentation: https://realpython.com/blog/python/primer-on-jinja-templating/
  #
  # You can see an example template in templates/index.html
  #
  # context are the variables that are passed to the template.
  # for example, "data" key in the context variable defined below will be
  # accessible as a variable in index.html:
  #
  #     # will print: [u'grace hopper', u'alan turing', u'ada lovelace']
  #     <div>{{data}}</div>
  #
  #     # creates a <div> tag for each element in data
  #     # will print:
  #     #
  #     #   <div>grace hopper</div>
  #     #   <div>alan turing</div>
  #     #   <div>ada lovelace</div>
  #     #
  #     {% for n in data %}
  #     <div>{{n}}</div>
  #     {% endfor %}
  #
  # context = dict(data = names)


  #
  # render_template looks in the templates/ folder for files.
  # for example, the below file reads template/index.html
  #
  return render_template("index.html")

#
# This is an example of a different path.  You can see it at:
#
#     localhost:8111/another
#
# Notice that the function name is another() rather than index()
# The functions for each app.route need to have different names
#



# Search movie
@app.route('/searchmovie', methods=['POST'])
def searchmovie():
  input = request.form['moviename']
  movie = g.conn.execute('''SELECT * FROM movie M, director D, country C,
  (SELECT M1.mid, ROUND(AVG(R.score)::numeric,2) AS ave
  FROM movie M1, rate R WHERE M1.mid = R.mid GROUP BY M1.mid) M2
  WHERE M.mname=%s AND M.did = D.did AND M.mid = M2.mid AND M.cid = C.cid
  ORDER BY M2.ave DESC''', input)

  other = g.conn.execute('''SELECT * FROM movie M, played_by P, actor A
  WHERE M.mname = %s AND M.mid = P.mid AND P.aid = A.aid
  ORDER BY P.rank''', input)

  genre = g.conn.execute('''SELECT * FROM movie M, belong_to B, genre G
  WHERE M.mname = %s AND M.mid = B.mid AND B.gid = G.gid''', input)

  movie_list = []
  item = movie.fetchone()
  while not item == None:
      movie_list.append(item)
      item = movie.fetchone()

  actor_list = []
  item = other.fetchone()
  while not item == None:
      actor_list.append(item)
      item = other.fetchone()

  genre_list = []
  item = genre.fetchone()
  while not item == None:
      genre_list.append(item)
      item = genre.fetchone()

  context = dict(data = movie_list, data1 = actor_list, data2 = genre_list)
  movie.close()
  other.close()
  genre.close()
  return render_template("movieresult.html", **context)


# Search director
@app.route('/searchDirector', methods=['POST'])
def search():
    input = request.form['Directorname']
    director = g.conn.execute('''SELECT D.did, D.dname, COUNT(*) AS count
    FROM director D, movie M
    WHERE M.did = D.did
    AND D.dname=%s GROUP BY D.did, D.dname''', input)

    movie = g.conn.execute('''SELECT D1.did, M1.mname, M1.rating, M1.year
    FROM (SELECT D.did FROM director D WHERE D.dname = %s) D1, movie M1
    WHERE D1.did = M1.did ORDER BY M1.year DESC''', input)

    director_list = []
    item = director.fetchone()
    while not item == None:
        director_list.append(item)
        item = director.fetchone()

    movie_list = []
    item = movie.fetchone()
    while not item == None:
        movie_list.append(item)
        item = movie.fetchone()

    context = dict(data = director_list, data1 = movie_list)
    director.close()
    movie.close()
    return render_template("directorresult.html",**context)

# choose movie based genre
@app.route('/chooseGenre', methods=['POST'])
def chooseGenre():
    input = request.form['genre']
    movie = g.conn.execute('''SELECT * FROM genre G, belong_to B, movie M, country C,
    (SELECT M1.mid, ROUND(AVG(R.score)::numeric,2) AS ave
    FROM movie M1, rate R WHERE M1.mid = R.mid GROUP BY M1.mid) M2
    WHERE G.gid = B.gid AND B.mid = M.mid AND M2.mid = M.mid AND C.cid = M.cid AND G.gname = %s
    ORDER BY M2.ave DESC''',input)

    other = g.conn.execute('''SELECT * FROM movie M, played_by P, actor A, genre G, belong_to B
    WHERE M.mid = P.mid AND P.aid = A.aid AND G.gid = B.gid AND B.mid = M.mid AND G.gname = %s
    ORDER BY P.rank''', input)



    movie_list = []
    item = movie.fetchone()
    count = 0
    while not (item == None or count > 50):
        movie_list.append(item)
        item = movie.fetchone()
        count = count + 1

    actor_list = []
    item = other.fetchone()
    while not item == None:
        actor_list.append(item)
        item = other.fetchone()

    context = dict(data = movie_list, data1 = actor_list, data2 = movie_list)
    movie.close()
    other.close()
    return render_template("movieresult.html", **context)

# submit a rate from user
@app.route('/rate',  methods=['POST'])
def rate():
    movieid = int(request.form['movie'])
    userid = int(request.form['userid'])
    score = float(request.form['score'])
    # check whether this userid is valid
    person = g.conn.execute ("SELECT * FROM users U WHERE U.uid = %s",userid)
    if not person.fetchone() == None:
        res = g.conn.execute("SELECT * FROM rate R WHERE R.mid = %s AND R.uid = %s", movieid, userid)
        if res.fetchone() == None:
            g.conn.execute("INSERT INTO rate VALUES(%s,%s,%s)", userid, movieid, score)
        else:
            g.conn.execute("UPDATE rate SET score = %s WHERE mid = %s AND uid = %s", score, movieid, userid)
        res.close()
        person.close()
        return 'You have successfully rate the movie'
    else:
        return 'This user does not exist!'



@app.route('/searchActor', methods=['POST'])
def searchActor():
    input = request.form['Actorname']
    actor = g.conn.execute('''SELECT A.aid, A.aname, COUNT(*) AS count FROM actor A, played_by P
    WHERE A.aid = P.aid AND A.aname = %s GROUP BY A.aid, A.aname''',input)

    movie = g.conn.execute('''SELECT P1.aid, M.mname, M.rating, M.year
    FROM (SELECT P.aid, P.mid FROM actor A, played_by P
    WHERE A.aid = P.aid AND A.aname = %s) P1, movie M
    WHERE P1.mid = M.mid ORDER BY M.year DESC''',input)

    actor_list = []
    item = actor.fetchone()
    while not item == None:
        actor_list.append(item)
        item = actor.fetchone()

    movie_list = []
    item = movie.fetchone()
    while not item == None:
        movie_list.append(item)
        item = movie.fetchone()

    context = dict(data = actor_list, data1 = movie_list)
    actor.close()
    movie.close()
    return render_template("actorresult.html",**context)


@app.route('/chooseArea', methods=['POST'])
def chooseArea():
    input = request.form['area']
    country = g.conn.execute('''SELECT C.cname FROM area A, country C
    WHERE A.rid = C.rid AND A.rname = %s''', input)
    country_list = []
    for result in country:
        country_list.append(result['cname'])
    country.close()
    context = dict(data1 = country_list, data2 = input)
    return render_template("country.html", **context)


@app.route('/chooseCountry', methods=['POST'])
def chooseCountry():
    input = request.form['country']
    movie = g.conn.execute('''SELECT * FROM country C, movie M, director D,
    (SELECT M1.mid, ROUND(AVG(R.score)::numeric,2) AS ave FROM movie M1, rate R WHERE M1.mid = R.mid GROUP BY M1.mid) M2
    WHERE C.cid = M.cid AND M.did = D.did AND M2.mid = M.mid AND C.cname = %s
    ORDER BY M2.ave DESC''', input)

    other = g.conn.execute('''SELECT * FROM country C, movie M, played_by P, actor A
    WHERE C.cid = M.cid AND M.mid = P.mid AND P.aid = A.aid AND C.cname = %s
    ORDER BY P.rank''', input)

    genre = g.conn.execute('''SELECT * FROM country C, movie M, belong_to B, genre G
    WHERE C.cid = M.cid AND M.mid = B.mid AND B.gid = G.gid AND C.cname = %s''', input)

    movie_list = []
    item = movie.fetchone()
    count = 0
    while not (item == None or count > 50):
        movie_list.append(item)
        item = movie.fetchone()
        count = count + 1

    actor_list = []
    item = other.fetchone()
    while not item == None:
        actor_list.append(item)
        item = other.fetchone()

    genre_list = []
    item = genre.fetchone()
    while not item == None:
        genre_list.append(item)
        item = genre.fetchone()

    movie.close()
    other.close()
    genre.close()
    context = dict(data = movie_list, data1 = actor_list, data2 = genre_list)
    return render_template("movieresult.html", **context)


@app.route('/recommend', methods=['POST'])
def recommend():
    input = request.form['method']
    user_id = int(request.form['userid'])
    # check whether this userid is valid
    person = g.conn.execute("SELECT * FROM users U WHERE U.uid = %s",user_id)
    if person.fetchone() == None:
        return 'This user does not exist!'

    if input == "age":
        user = g.conn.execute("SELECT age FROM users WHERE uid = %s", user_id)
        user_age = int(user.fetchone()['age'])
        print user_age

        movie = g.conn.execute('''SELECT M.mid, M.mname, M.year, M.rating, ROUND(AVG(R.score)::numeric,2) AS ave
        FROM users U, rate R, movie M
        WHERE U.age = %s AND U.uid = R.uid AND R.mid = M.mid AND U.uid != %s
        GROUP BY M.mid, M.mname, M.year, M.rating HAVING AVG(R.score) > 4  AND COUNT(R.score) > 20
        ORDER BY ave DESC''', user_age, user_id)

        movie_list = []
        item = movie.fetchone()
        while not (item == None):
            movie_list.append(item)
            item = movie.fetchone()
        context = dict(data = movie_list, data1 = input)
        user.close()
        movie.close()
        return render_template("recommendation.html", **context)

    elif input == "gender":
        user = g.conn.execute("SELECT gender FROM users WHERE uid = %s", user_id)
        user_gender = user.fetchone()['gender']
        print user_gender

        movie = g.conn.execute('''SELECT M.mid, M.mname, M.year, M.rating, ROUND(AVG(R.score)::numeric,2) AS ave
        FROM users U, rate R, movie M
        WHERE U.gender = %s AND U.uid = R.uid AND R.mid = M.mid AND U.uid != %s
        GROUP BY M.mid, M.mname, M.year, M.rating HAVING AVG(R.score) > 4 AND COUNT(R.score) > 20
        ORDER BY ave DESC''',user_gender, user_id)

        movie_list = []
        item = movie.fetchone()
        while not (item == None):
            movie_list.append(item)
            item = movie.fetchone()
        context = dict(data = movie_list, data1 = input)
        user.close()
        movie.close()
        return render_template("recommendation.html", **context)

    elif input == "occupation":
        user = g.conn.execute("SELECT occupation FROM users WHERE uid = %s", user_id)
        user_job = user.fetchone()['occupation']
        print user_job

        movie = g.conn.execute('''SELECT M.mid, M.mname, M.year, M.rating, ROUND(AVG(R.score)::numeric,2) AS ave
        FROM users U, rate R, movie M
        WHERE U.occupation = %s AND U.uid = R.uid AND R.mid = M.mid AND U.uid != %s
        GROUP BY M.mid, M.mname, M.year, M.rating HAVING AVG(R.score) > 4  AND COUNT(R.score) > 20
        ORDER BY ave DESC''',user_job, user_id)

        movie_list = []
        item = movie.fetchone()
        while not (item == None):
            movie_list.append(item)
            item = movie.fetchone()
        context = dict(data = movie_list, data1 = input)
        user.close()
        movie.close()
        return render_template("recommendation.html", **context)


@app.route('/signup', methods=['POST'])
def signup():
    input = request.form['newid']
    user = g.conn.execute("SELECT * FROM users WHERE uid = %s", input)
    if not user.fetchone() == None:
        return 'This name has been taken, please go back and choose another ID'
    else:
        age = request.form['age']
        job = request.form['job']
        gender = request.form['gender']
        g.conn.execute("INSERT INTO users VALUES(%s,%s,%s,%s)",input,age,gender,job)
        return 'Successfully sign up'


if __name__ == "__main__":
  import click

  @click.command()
  @click.option('--debug', is_flag=True)
  @click.option('--threaded', is_flag=True)
  @click.argument('HOST', default='0.0.0.0')
  @click.argument('PORT', default=8111, type=int)
  def run(debug, threaded, host, port):
    """
    This function handles command line parameters.
    Run the server using:

        python server.py

    Show the help text using:

        python server.py --help

    """

    HOST, PORT = host, port
    print "running on %s:%d" % (HOST, PORT)
    app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)


  run()
