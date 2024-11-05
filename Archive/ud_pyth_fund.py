# Databricks notebook source
import sys
lst =sys.argv
for i in lst:
  print(i)

# COMMAND ----------

# def display(name):
#   def message():
#     return f"Hello "
#   return message() + name

# print(display('sravan'))  

# def display(fun):
#   return "hello " + fun


# def message():
#   return "Bharath"

# print(display(message()))

# def fun(lst):
#   for i in lst:
#     print(i)

# fun([1,2,3,4,5])

# def factorial(n):
#   if n == 0:
#     return
#   else:
#     return n * factorial(n-1)
  
# print(factorial(4))

def calbmi(height,weight):
  heightinmeters = height * 0.0254
  bmi = weight / (heightinmeters * heightinmeters)
  return bmi

print(calbmi(5.8,73))

# COMMAND ----------

#lambda functions -- lambda argument_list: expressions
f= lambda x,y: x+y
print(f(10,20))

def cube(n):
  return n**3

print(cube(4))

f =lambda n:n**3
print(f(4))


l =lambda x: "yes" if x>0 else "no"
print(l(10))

even = lambda x: x%2==0



# COMMAND ----------

lst =[10,2,3,44,56]
result =filter(lambda x: x%2==0,lst)
print(list(result))




# COMMAND ----------

class Product:
    def __init__(self, name,description):
        self.name =name
        self.description =description
        
c1 = Product("Android","its Awesome")
print(c1.name)


# COMMAND ----------

class Course:
  def __init__(self, name, ratings):
    self.name = name
    self.ratings = ratings
  
c1= Course("Python", [4.5, 4.0, 4.2])
print(c1.ratings)

# COMMAND ----------

import gc
print(gc.isenabled())
