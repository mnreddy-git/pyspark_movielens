import yaml
import os
import sys
#from __future__ import print


def main():
    project_path = os.path.dirname(__file__)
    print("project_path is : ",project_path)
    conf_path = os.path.abspath(os.path.join(project_path, '..', 'conf', 'MovieLens.yaml'))
    print("conf_path is : ",conf_path)
    with open(conf_path, 'r') as conf_stream:
        try:
            conf = yaml.load(conf_stream)
            pipe_delim=conf['movieLens']['delimiters']['PIPE_DELIMITER']
            dollar_delim = conf['movieLens']['delimiters']['DOLLAR_DELIMITER']
            print(type(pipe_delim))
            print(pipe_delim)
            print(dollar_delim)
            hfalse=conf['movieLens']['options']['HEADER_FALSE']
            htrue=conf['movieLens']['options']['HEADER_TRUE']
            print(hfalse)
            print(htrue)
            print(type(htrue))
            print(type(hfalse))
            stringtype = conf['movieLens']['types']['STRING_TYPE']
            print(stringtype)
            print(type(stringtype))
        except yaml.YAMLError as e:
            print("Exception is ",e)
if __name__=="__main__":
    main()

