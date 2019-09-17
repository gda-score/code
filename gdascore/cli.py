# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

from pprint import pprint
from PyInquirer import style_from_dict, Token, prompt
from PyInquirer import Validator, ValidationError
from pyfiglet import Figlet

import os, sys
from time import sleep

try:
    from .sampleConfig import readme, myCredentials
except ImportError:
    from sampleConfig import readme, myCredentials


def init():
    print()
    f = Figlet(font='slant')
    print(f.renderText('GDA Score'))

    print(
        'Welcome to GDA Score project. for GDA Score module to function properly, you need to provide your desired ' +
        '"results" and "config" folder through this initiation procedure.')
    print()

    style = style_from_dict({
        Token.QuestionMark: '#E91E63 bold',
        Token.Selected: '#673AB7 bold',
        Token.Instruction: '#FF0000',  # default
        Token.Answer: '#2196f3 bold',
        Token.Question: '#fff000',
    })

    class PathValidator(Validator):
        def validate(self, document):
            ok = os.path.exists(document.text)
            if not ok:
                raise ValidationError(
                    message='path does not exist. please enter a valid path.',
                    cursor_position=len(document.text))  # Move cursor to end

    questions = [
        {
            'type': 'input',
            'name': 'resPath',
            'message': 'where should "results" folder be placed? please provide absolute path.',
            'validate': PathValidator,
            'default': os.path.abspath(os.getcwd())
        },
        {
            'type': 'list',
            'name': 'boolCrtResFld',
            'message': 'there is no folder named "results" in the path specified, it will be created. ' +
                       'hit enter to continue.',
            'choices': ['ok'],
            'when': lambda ans: not os.path.exists(os.path.join(ans['resPath'], 'results'))
        },
        {
            'type': 'input',
            'name': 'cnfPath',
            'message': 'where should "config" folder be placed? please provide absolute path.',
            'validate': PathValidator,
            'default': os.path.abspath(os.getcwd())
        },
        {
            'type': 'list',
            'name': 'boolCrtCnfFld',
            'message': 'there is no folder named "config" in the path specified, it will be created. ' +
                       'hit enter to continue',
            'choices': ['ok'],
            'when': lambda ans: not os.path.exists(os.path.join(ans['cnfPath'], 'config'))
        }
    ]

    answers = prompt(questions, style=style)
    print()
    main(**answers)
    print('\ninitiation done. you can start using the code.\n')


def main(cnfPath, resPath, boolCrtResFld=None, boolCrtCnfFld=None):
    if boolCrtCnfFld:
        createConfigFolder(cnfPath)
    if boolCrtResFld:
        createResultFolder(resPath)
    propagateSampleConfig(cnfPath)
    setGlobalVariables(cnfPath, resPath)


def createResultFolder(resPath):
    try:
        os.mkdir(os.path.join(resPath, 'results'))
    except IOError as ex:
        print(f'result folder could not be created due to an error: {ex}')
        print('Initiation failed. Fix the error and start procedure again.\n')
        exit(-1)
    else:
        print(' > result folder created successfully. ')
        sleep(1)


def createConfigFolder(cnfPath):
    try:
        os.mkdir(os.path.join(cnfPath, 'config'))
    except IOError as ex:
        print(f'result folder could not be created due to an error: {ex}')
        print('Initiation failed. Fix the error and start procedure again.\n')
        exit(-1)
    else:
        print(' > config folder created successfully. ')
        sleep(1)


def propagateSampleConfig(cnfPath):
    # common/config/*
    # copy file contents ino variables and then make files here
    try:
        with open(os.path.join(cnfPath, 'config', 'myCredentials.json'), 'w') as f:
            f.write(myCredentials)
        # with open(os.path.join(cnfPath, 'config', 'README.md'), 'w') as f:
        #     f.write(readme)
    except IOError as e:
        # print("I/O error({0}): {1}".format(e.errno, e.strerror))
        print(f'propagating default config files failed due to an error: {e} \n')
        print('Initiation failed. Fix the error and start procedure again.\n')
        exit(-1)

    except:  # handle other exceptions such as attribute errors
        print("Unexpected error:", sys.exc_info()[0])
        exit(-1)
    else:
        print(' > default config files propagated successfully.')
        sleep(1)


def setGlobalVariables(cnfPath, resPath):
    res = f'''{{
        "config_path": "{cnfPath}",
        "result_path": "{resPath}"
}}'''
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'global_config', 'config_var.json'),
                  'w') as f:
            f.write(res)
    except IOError as e:
        # print("I/O error({0}): {1}".format(e.errno, e.strerror))
        print(f'setting global configuration variables failed due to an error: {e} \n')
        print('Initiation failed. Fix the error and start procedure again.\n')
        exit(-1)
    else:
        print(' > global configuration variables set successfully.')
        sleep(1)


if __name__ == "__main__":
    init()
