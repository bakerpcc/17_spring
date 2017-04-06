import json

file_out=open('output/trainingBusiness.json','w')
with open("yelp/trainingBusiness.json") as infile:
    j = json.load(infile)
    data = {}
    for i in j:
        #print i
        data[i['business_id']]={'stars':i['stars'],'name':i['name'],'review_count':i['review_count'],\
                                'categories':i['categories'],'city':i['city']}
    json.dump(data, file_out, indent=4)


file_out=open('output/trainingUser.json','w')
with open("yelp/trainingUser.json") as infile:
    j = json.load(infile)
    data = {}
    for i in j:
        #print i
        data[i['user_id']]={'review_count':i['review_count'],'average_stars':i['average_stars'],\
                            'votes':i['votes']}
    json.dump(data, file_out, indent=4)



file_out=open('output/trainingReview.json','w')
with open("yelp/trainingReview.json") as infile:
    j = json.load(infile)
    data = {}
    for i in j:
        #print i
        data[str(i['user_id'])+','+str(i['business_id'])]=i['stars']
    json.dump(data, file_out, indent=4)

