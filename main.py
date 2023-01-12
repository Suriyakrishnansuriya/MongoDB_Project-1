import pymongo as py
import json


Client=py.MongoClient('mongodb://localhost:27017')
mydbs=Client.list_database_names()
db=Client['project']
student=db['students.record']
collection=db.list_collection_names()
print(collection)
'''file=open('students.json')
data=json.load(file)
student.insert_many(data)'''

print('------------------------------------------------------------------')
#1 maximum marks scores in all(exam,quiz,homework)
print('maximum marks in all categories')
for i in student.aggregate([{'$group':{'max_marks in exam,quiz,homework':{'$max':'$scores.score'}, '_id':'$name'}},
                                    {'$sort':{'max_marks in exam,quiz,homework':-1}},{'$limit':1}]):
    print(i)


#2 below average in the exam and pass mark is 40?
print('below average in the exam and pass mark is 40')
for i in student.aggregate([{'$unwind':'$scores'},{'$match':{'scores.type':'exam'}},{'$group':{'_id':'$name','average':{'$avg':'$scores.score'}}},
                             ]):
    print(i)

#3 find student who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories
print('failed students')
for i in student.find({'$or':[{'scores':{'$elemMatch':{'type':"exam", 'score':{'$lt':40}}}},
                                {'scores':{'$elemMatch':{'type':"quiz", 'score':{'$lt':40}}}},
                                {'scores':{'$elemMatch':{'type':"homework", 'score':{'$lt':40}}}}]}):
    print(i)
print('passed students in all categories')
for i in student.find({'$and':[{'scores':{'$elemMatch':{'type':"exam", 'score':{'$gt':40}}}},
                                {'scores':{'$elemMatch':{'type':"quiz", 'score':{'$gt':40}}}},
                                {'scores':{'$elemMatch':{'type':"homework", 'score':{'$gt':40}}}}]}):
    print(i)

#4 find the total and average of the exam,quiz,homework and store them seperate collection
print('total in all categories ')
student.aggregate([{'$unwind':'$scores'},{'$group':{'_id':'$scores.type','total_score':{'$sum':'$scores.score'}}},{'$out':'total_in_AllCategories'}])
for i in db.total_in_AllCategories.find():
    print(i)

print('average of all categories')
student.aggregate([{'$unwind':'$scores'},{'$group':{'_id':'$scores.type','average_score':{'$avg':'$scores.score'}}},{'$out':'average_of_AllCategories'}])
for i in db.average_of_AllCategories.find():
    print(i)
#5 cretae a new collection which consists of
# students who scored below average and above 40% in all the categories
print('New Collection Created Successfully')
print('students who scored below average and above 40% in all the categories')
student.aggregate([{'$unwind':'$scores'},{'$group':{'avg_marks in exam,quiz,homework':{'$avg':'$scores.score'}, '_id':'$name'}},
                             {'$out':'below_average'}])
for i in db.below_average.find():
    print(i)

#6 create a new collection which consists of student who scored below the fail mark in all categories

print('New Collection Created Successfully')
print('student who scored below the fail mark in all categories')
fail_allcategories=db['fail_allcategories']
'''db.fail_allcategories.insert_many(student.find({'$and':[{'scores':{'$elemMatch':{'type':"exam", 'score':{'$lt':40}}}},
                                {'scores':{'$elemMatch':{'type':"quiz", 'score':{'$lt':40}}}},
                                {'scores':{'$elemMatch':{'type':"homework", 'score':{'$lt':40}}}}]}))'''
for i in fail_allcategories.find():
    print(i)

#7 create a new collection which consists of students who scored above pass mark in all the categories
print('New Collection Created Successfully')
print('Above 40 marks in each categories')
pass_allcategories=db['pass_allcategories']
'''db.pass_allcategories.insert_many(student.find({'$and':[{'scores':{'$elemMatch':{'type':"exam", 'score':{'$gt':40}}}},
                                {'scores':{'$elemMatch':{'type':"quiz", 'score':{'$gt':40}}}},
                                {'scores':{'$elemMatch':{'type':"homework", 'score':{'$gt':40}}}}]}))'''
for i in db.pass_allcategories.find():
    print(i)
