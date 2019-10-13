import tensorflow as tf
from tensorflow import keras
from dataworkspaces.kits.tensorflow1 import add_lineage_to_keras_model_class


@add_lineage_to_keras_model_class
class MyModel(keras.Model):
  def __init__(self):
    print("In MyModel init")
    #super(MyModel, self).__init__()
    super().__init__()
    self.dense1 = tf.keras.layers.Dense(4, activation=tf.nn.relu)
    self.dense2 = tf.keras.layers.Dense(5, activation=tf.nn.softmax)

  def call(self, inputs):
    print("Inputs: %s" % repr(inputs))
    x1 = self.dense1(inputs)
    return self.dense2(x1)

model = MyModel()

import numpy as np

print("compiling model")
model.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['accuracy'])
print("model fitting")
model.fit(np.zeros(20).reshape((5,4)), np.ones(5), epochs=5)
print("evaluating model")
test_loss, test_acc = model.evaluate(np.zeros(16).reshape(4,4), np.ones(4))

print('Test accuracy:', test_acc)
