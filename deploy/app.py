import pandas as pd
from flask import Flask, jsonify,request,render_template
import joblib
import json
import sqlite3
import datetime
from pandas import json_normalize
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
app = Flask(__name__)

#Get Input JSON and convert to dataframe. Do Data proprocessing and send back reult as json object. Store the result in table 
@app.route('/predict', methods = ['POST'])
def predict():
	req = request.get_json()
	input_data = req['data']
	model=joblib.load(r'Model/model.pkl')
    
	#inputdata = '{"objid":"12376000", "ra":"122.4725359488", "dec":"29.4775464207", "u":"18.80104", "g":"18.79409", "r":"19.17904", "i":"19.46893", "z":"19.62121", "run":"2830", "rerun":"301", "camcol":"1", "field":"82", "specobjid":"5011613674718380000", "redshift":"500.0001133478", "plate":"4451", "mjd":"55537", "fiberid":"848"}'
	df2 = pd.DataFrame.from_dict(input_data,orient='index').T
	df2.fillna(df2.mean().round(1), inplace=True)
	df2.drop(['specobjid', 'fiberid','objid','rerun'], axis='columns', inplace=True)
	for col in ['ra','dec','u','g','r','i','z','run','camcol','field','redshift','plate','mjd']:
		df2[col] = df2[col].astype(float)
	df2['redshift_class'] = df2['redshift'].apply(lambda x: True if x>=0 else False)
	df2[['i','z']] = df2[['i','z']].clip(7,24)
	# Making relationships from existing features baeed on pair plot distribution
	df2['u_g'] = df2['u'] - df2['g']
	df2['g_r'] = df2['g'] - df2['r']
	df2['r_i'] = df2['r'] - df2['i']
	df2['i_z'] = df2['i'] - df2['z']

	scaler = StandardScaler()
	scaler.fit(df2)
	pred=model.predict(df2.values)
	print(pred)
	astromap = {'0':'STAR', '1':'GALAXY', '2': 'QSO'}
	prediction = astromap.get(str(pred[0]),'UNKNOWN')
	try:
		with sqlite3.connect("database.db") as con:
			cur = con.cursor()
			cur.execute("INSERT INTO prediction (Timestmp,objectid,ra,decc,u,g,r,i,z,run,camcol,field,redshift,plate,mjd,output) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",(datetime.datetime.now(),req['data']['objid'],req['data']['ra'],req['data']['dec'],req['data']['u'],req['data']['g'],req['data']['r'],req['data']['i'],req['data']['z'],req['data']['run'],req['data']['camcol'],req['data']['field'],req['data']['redshift'],req['data']['plate'],req['data']['mjd'],prediction ) )
			con.commit()
			msg = "Record successfully added"
	except sqlite3.Error as error:
		print("Error while inserting with SQLite", error)
	finally:
		if (con):
			con.close()
			print("sqlite connection is closed")
	return jsonify({'output':{'object' : prediction}})

#function for History prediction results. Fetching from table preidction 
@app.route('/showhistoryprediction', methods = ['GET'])
def list():
	try:
		con = sqlite3.connect("database.db")
		con.row_factory = sqlite3.Row

		cur = con.cursor()
		cur.execute("select * from prediction")

		rows = cur.fetchall(); 
		return render_template("historypage.html",rows = rows)
	except sqlite3.Error as error:
		print("Error while working with SQLite", error)
	finally:
		if (con):
			con.close()
			print("sqlite connection is closed")


@app.route('/')
def home():

	return render_template("homepage.html")
	

if __name__=='__main__':
	app.run(host= '0.0.0.0', port ='3000')

