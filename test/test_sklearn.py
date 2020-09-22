import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression



df = pd.DataFrame([
            {'gender':'Male', 'group':1},
            {'gender':'Female', 'group':3},
            {'gender':'Female', 'group':2}])

enc = OneHotEncoder(handle_unknown='ignore',sparse=False)
enc.fit(df)
enc.categories_
df2 = enc.transform(df)
print(df2)
df3 = enc.transform(df)

r =  np.concatenate((df2,df3), axis=1)
print(r)
enc.inverse_transform([[0, 1, 1, 0, 0], [0, 0, 0, 1, 0]])
enc.get_feature_names(['gender', 'group'])

print(enc.transform([['Female', 1], ['Male', 4]]))
df = pd.DataFrame(enc.transform([['Female', 1], ['Male', 4]]))
print(df)

clf = LogisticRegression(random_state=0).fit(r, [1,0,1])
print(clf.predict(r[:2, :]))