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

from kimi_common.utils.logger import getLogger

logger = getLogger(__name__)

class SklearnBinaryClassificationTrainer():

    def __init__(self,model_dir_path,cate_features,number_features,negtive_sample_ratio=2,
                test_split_mode='last',test_split_ratio=0.2,true_value=1,label='label',cate_nan_value='nan'):
        self.model_dir_path = model_dir_path
        self.onehot_model_path = os.path.join(self.model_dir_path,'onehot_encoder.pkl')
        self.strandard_scaler_model_path = os.path.join(self.model_dir_path,'strandard_scaler.pkl')
        self.lr_model_path = os.path.join(self.model_dir_path,'lr.pkl')
        self.cate_features = cate_features
        self.number_features = number_features
        self.negtive_sample_ratio= negtive_sample_ratio
        self.test_split_mode = test_split_mode
        self.test_split_ratio = test_split_ratio
        self.true_value = true_value
        self.label = label
        self.cate_nan_value = cate_nan_value


        if os.path.exists(self.onehot_model_path):
            with open(self.onehot_model_path, 'rb') as f:
                self.onehot_encoder = pickle.load(f)
        else:
            self.onehot_encoder = OneHotEncoder(handle_unknown='ignore',sparse=False)
        
        if os.path.exists(self.strandard_scaler_model_path):
            with open(self.strandard_scaler_model_path, 'rb') as f:
                self.standard_scaler = pickle.load(f)
        else:
            self.standard_scaler = StandardScaler()
            
        
        if os.path.exists(self.lr_model_path):
            with open(self.lr_model_path, 'rb') as f:
                self.lr_model = pickle.load(f)
        else:
            self.lr_model = LogisticRegression(random_state=0)


    def negtive_sample(self,df,need_shuffle=True):
        true_df = df[df.label==self.true_value]
        false_df = df[df.label!= self.true_value]
        false_orig_size = len(false_df)
        false_df = false_df.sample(false_orig_size* self.negtive_sample_ratio)
        sampled_df = pd.concat([true_df,false_df])
        if need_shuffle:
            sampled_df = shuffle(sampled_df)
        sample_rate = len(false_df) /false_orig_size

        logger.info(f'true size:{len(true_df)} false size:{false_orig_size} final false size:{false_orig_size * self.negtive_sample_ratio}')
        logger.info(f'negtive sample ratio:{self.negtive_sample_ratio} final sample rate:{sample_rate}')

        return sampled_df,sample_rate

    def data_split(self,df):
        total_size =  len(df)
        test_split_count = total_size * self.test_split_ratio
        train_df = df[:-test_split_count]
        test_df = df[-test_split_count:]
        return train_df,test_df

    def save_model(self):
        with open(self.onehot_model_path, 'wb') as f:
            pickle.dump(self.onehot_encoder, f)
        
        with open(self.strandard_scaler_model_path, 'wb') as f:
            pickle.dump(self.standard_scaler, f)

        with open(self.lr_model_path, 'wb') as f:
            pickle.dump(self.lr_model, f)

    
    def train(self,df):
        df = df[self.cate_features + self.number_features +  [self.label]]
        df[self.cate_features] = df[self.cate_features].fillna(self.cate_nan_value)

        ## 
        # sample_rate 用来后续做校正
        #
        sampled_df , sample_rate = self.negtive_sample(df)
        train_df,test_df  = self.data_split(sampled_df)

        logger.info(f'training count:{len(train_df)}')
        logger.info(f'test count:{len(test_df)}')

        self.onehot_encoder.fit(train_df[self.cate_features])
        self.standard_scaler.fit(train_df[self.number_features])

        train_cate_features =  self.onehot_encoder.transform(train_df[self.cate_features])
        train_number_features  = self.standard_scaler.transform(train_df[self.number_features])
        train_features =  np.concatenate((train_cate_features,train_number_features), axis=1)

        self.lr_model.fit(train_features, train_df[self.label])

        test_cate_features_df = self.onehot_encoder.transform(test_df[self.cate_features])
        test_number_featuers_df  = self.standard_scaler.transform(test_df[self.number_features])
        test_features =  np.concatenate((test_cate_features_df,test_number_featuers_df), axis=1)

        logger.info(f'train acc:{self.lr_model.score(train_features,train_df[self.label])}')
        logger.info(f'test acc:{self.lr_model.score(test_features,test_df[self.label])}')

        # train auc
        train_predict_score = self.lr_model.decision_function(train_features) # 计算属于各个类别的概率，返回值的shape = [n_samples, n_classes]
        train_auc = roc_auc_score(train_df[self.label].values, train_predict_score, average='micro')
        logger.info(f'train auc：{train_auc}')

        # test auc
        test_predict_score = self.lr_model.decision_function(test_features) # 计算属于各个类别的概率，返回值的shape = [n_samples, n_classes]
        test_auc = roc_auc_score(test_df[self.label], test_predict_score, average='micro')
        logger.info(f'test auc:{test_auc}')


        self.save_model()



    
    def predict(self,df):
        start =time.time()
        df[self.cate_features] = df[self.cate_features].fillna(self.cate_nan_value)
        cate_features_df = self.onehot_encoder.transform(df[self.cate_features])
        number_featuers_df  = self.standard_scaler.transform(df[self.number_features])
        features =  np.concatenate((cate_features_df,number_featuers_df), axis=1)
        df[['predict0','predict1']]= self.lr_model.predict_proba(features)
        end = time.time()
        logger.info(f'predicted data elasped:{end-start}')
        return df





