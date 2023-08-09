# TASK:
# 1. validate data ie check for datatype, data ranges,null
# a. validation checks include:
#     1a. if overall ranges from 1 to 5
#     2a. if reviewr_id is null
#     3a. if reviewer_name>2    
# 2. if all checks pass then load data to db
# 3. else load audit to audit logs with exception
# 4. check data metric and if threshold more then generate email


class validate_data:

    data = {}
    errors = []
    status = False
    
    def __init__(self):
      pass

    def validate_all_checks(self,data):

        self.status = False
        self.errors = []

        overall = data['overall']
        reviewText = data['reviewText']
        review_id = data['reviewerID']
       
        self.validate_overall(overall)
        self.validate_review_id(review_id)
        self.validate_reviewer_text(reviewText)

        if len(self.errors) == 0:

            self.status = True
        
        else:
            self.status = False

        resultant_dictionary = {'status': self.status ,'errors': self.errors}

        return resultant_dictionary


    def validate_overall(self,overall):
        
        if overall <1 or overall >5:
            self.errors.append('overall value out of range')

       
    
    def validate_review_id(self,review_id):

        if review_id is None or review_id == '': 
            self.errors.append('review id is empty')


    
    def validate_reviewer_text(self,reviewText):

        if reviewText is None or reviewText == '': 
            self.errors.append('review text is empty')
    
