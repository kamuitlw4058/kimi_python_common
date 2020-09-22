import time
import pandas as pd
import numpy as np
import os
import pickle
from multiprocessing import Pool

from sklearn.utils import shuffle as shuffle

from sklearn.preprocessing import StandardScaler,OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline,Pipeline
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score,roc_auc_score

from sklearn_pandas import DataFrameMapper

class SklearnBinaryClassificationTrainer():

    def __init__(self,path):
        self.path = path
        self.onehot_model_path = os.path.join(self.path,'onehot_encoder.pkl')
        if os.path.exists(self.onehot_model_path):
            with open(self.onehot_model_path, 'rb') as f:
                self.onehot_encoder = pickle.load(f)
        else:
            self.onehot_encoder = OneHotEncoder(handle_unknown='ignore',sparse=False)
        
        self.strandard_scaler_model_path = os.path.join(self.path,'strandard_scaler.pkl')
        if os.path.exists(self.strandard_scaler_model_path):
            with open(self.strandard_scaler_model_path, 'rb') as f:
                self.standard_scaler = pickle.load(f)
        else:
            self.standard_scaler = StandardScaler()
            
        
        self.lr_model_path = os.path.join(self.path,'lr.pkl')
        if os.path.exists(self.lr_model_path):
            with open(self.lr_model_path, 'rb') as f:
                self.lr_model = pickle.load(f)
        else:
            self.lr_model = LogisticRegression(random_state=0)


    def negtive_sample(self,df,negtive_sample_ratio,true_value,need_shuffle=True):
        true_df = df[df.label==true_value]
        false_df = df[df.label!= true_value]
        false_orig_size = len(false_df)
        false_df = false_df.sample(len(true_df)* negtive_sample_ratio)
        sampled_df = pd.concat([true_df,false_df])
        if need_shuffle:
            sampled_df = shuffle(sampled_df)
        sample_rate = len(false_df) /false_orig_size
        print(f'true size:{len(true_df)} false size:{false_orig_size} negtive sample ratio:{negtive_sample_ratio} final sample rate:{sample_rate}')
        return sampled_df,sample_rate

    def data_split(self,df,test_split_number):
        train_df = df[:-test_split_number]
        test_df = df[-test_split_number:]
        return train_df,test_df

    
    def train(self,df,cate_features,number_features,keep_list,nagtive_sample=2,test_split_mode='last',test_split_number=10000,true_value=1,label='label'):
        df = df[cate_features + number_features+ keep_list +  [label]]
        df[cate_features] = df[cate_features].fillna(-1)
        sampled_df , sample_rate = self.negtive_sample(df,nagtive_sample,true_value)
        train_df,test_df  = self.data_split(sampled_df,test_split_number)

        print(f'training count:{len(train_df)}')
        print(f'test count:{len(test_df)}')


        self.onehot_encoder.fit(train_df[cate_features])
        self.standard_scaler.fit(train_df[number_features])

        train_cate_features =  self.onehot_encoder.transform(train_df[cate_features])
        train_number_features  = self.standard_scaler.transform(train_df[number_features])
        train_features =  np.concatenate((train_cate_features,train_number_features), axis=1)

        self.lr_model.fit(train_features, train_df[label])
        print(self.lr_model.score(train_features,train_df[label]))
        train_predict_score = self.lr_model.decision_function(train_features) # 计算属于各个类别的概率，返回值的shape = [n_samples, n_classes]
        # 1、调用函数计算micro类型的AUC
        print(train_predict_score)
        print(train_df[label])
        train_auc = roc_auc_score(train_df[label].values, train_predict_score, average='micro')
        
        print(f'train auc：{train_auc}')
        print(f'train acc:{self.lr_model.score(train_features,train_df[label])}')

        test_cate_features_df = self.onehot_encoder.transform(test_df[cate_features])
        test_number_featuers_df  = self.standard_scaler.transform(test_df[number_features])
        test_features =  np.concatenate((test_cate_features_df,test_number_featuers_df), axis=1)

        test_predict_score = self.lr_model.decision_function(test_features) # 计算属于各个类别的概率，返回值的shape = [n_samples, n_classes]
        test_auc = roc_auc_score(test_df[label], test_predict_score, average='micro')
        print(f'test auc:{test_auc}')
        print(f'test acc:{self.lr_model.score(test_features,test_df[label])}')
        print(self.lr_model.predict_proba(test_features))

        
        
        # for p,l,pt in zip(self.lr_model.predict_proba(test_features),test_df[label],self.lr_model.predict(test_features)):
        #     print(f'p:{p} l:{l} ,pt:{pt}')

        with open(self.onehot_model_path, 'wb') as f:
            pickle.dump(self.onehot_encoder, f)
        
        with open(self.strandard_scaler_model_path, 'wb') as f:
            pickle.dump(self.standard_scaler, f)

        with open(self.lr_model_path, 'wb') as f:
            pickle.dump(self.lr_model, f)

    
    def predict(self,df,cate_features,number_features):
        start =time.time()
        df[cate_features] = df[cate_features].fillna(-1)
        cate_features_df = self.onehot_encoder.transform(df[cate_features])
        number_featuers_df  = self.standard_scaler.transform(df[number_features])
        features =  np.concatenate((cate_features_df,number_featuers_df), axis=1)
        end = time.time()
        print(f'elasped test:{end-start}')
        df[['predict0','predict1']]= self.lr_model.predict_proba(features)
        return df





