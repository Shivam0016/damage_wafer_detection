from flask import Flask, render_template, request

app=Flask(__name__)

@app.route('/',methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/train',methods=["POST"])
def trainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            print(request.json['folderPath'])
    except Exception as ex:
        print(ex)

if __name__=="__main__":
    app.run()