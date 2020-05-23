import os
import keras
import numpy as np
from keras.datasets import fashion_mnist
import json
import time
import argparse, os
import numpy as np

import tensorflow as tf
import keras
from keras import backend as K
from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation, Flatten, BatchNormalization, Conv2D, MaxPooling2D
from keras.optimizers import SGD
from keras.utils import multi_gpu_model

def get_model_memory_usage(batch_size, model):
    import numpy as np
    from keras import backend as K

    shapes_mem_count = 0
    internal_model_mem_count = 0
    for l in model.layers:
        layer_type = l.__class__.__name__
        if layer_type == 'Model':
            internal_model_mem_count += get_model_memory_usage(batch_size, l)
        single_layer_mem = 1
        for s in l.output_shape:
            if s is None:
                continue
            single_layer_mem *= s
        shapes_mem_count += single_layer_mem
    trainable_count = np.sum([K.count_params(p) for p in model.trainable_weights])
    non_trainable_count = np.sum([K.count_params(p) for p in model.non_trainable_weights])

    number_size = 4.0
    if K.floatx() == 'float16':
         number_size = 2.0
    if K.floatx() == 'float64':
         number_size = 8.0

    total_memory = number_size*(batch_size*shapes_mem_count + trainable_count + non_trainable_count)
    gbytes = np.round(total_memory / (1024.0 ** 3), 3) + internal_model_mem_count
    return gbytes

# Given parameters 
training_dir = 'data/'
validation_dir = 'data/'
batch_size = 128
epochs = 20

# Variable from environment
memory_available = 16

# Determine if this is the first time
try:
    with open('recorder.json') as json_file:
        recorder = json.load(json_file)
    firstFlag = False
except FileNotFoundError:
    firstFlag = True
    recorder = {}

# loading data first
d = np.load(os.path.join('data.npz'))
x_train = d['x_train']
y_train = d['y_train']
x_val  = d['x_val']
y_val  = d['y_val']

# By default, TensorFlow maps nearly all of the GPU memory of all GPUs visible to the process. This is done to more efficiently use the relatively precious GPU memory resources on the devices by reducing memory fragmentation.

if firstFlag:
    # Safety checking
    # Memory
    new_model = keras.models.load_model('model.h5')
    while batch_size > 2:
        if get_model_memory_usage(batch_size, new_model) < memory_available:
            break
        else:
            batch_size //= 2
    if batch_size == 2:
        pass # Should notify memory warning
    recorder['batch_size'] = batch_size
    recorder['epochs'] = 0
    recorder['total_epoch'] = epochs
    recorder['estimate time for next iteration'] = None
    recorder['estimate time for training'] = None
    recorder['history result'] = []
    estimate_time_for_next_iteration = None
    estimate_time_for_training = None
else:
    new_model = keras.models.load_model('model.h5')
    batch_size = recorder['batch_size']
    epochs = recorder['total_epoch'] - recorder['epochs']
    estimate_time_for_next_iteration = recorder['estimate time for next iteration']
    estimate_time_for_training = recorder['estimate time for training']
            


class CustomModelCheckpoint(tf.keras.callbacks.Callback):
    """Save the model after every epoch.
    `filepath` can contain named formatting options,
    which will be filled with the values of `epoch` and
    keys in `logs` (passed in `on_epoch_end`).
    For example: if `filepath` is `weights.{epoch:02d}-{val_loss:.2f}.hdf5`,
    then the model checkpoints will be saved with the epoch number and
    the validation loss in the filename.
    # Arguments
        filepath: string, path to save the model file.
        monitor: quantity to monitor.
        verbose: verbosity mode, 0 or 1.
        save_best_only: if `save_best_only=True`,
            the latest best model according to
            the quantity monitored will not be overwritten.
        save_weights_only: if True, then only the model's weights will be
            saved (`model.save_weights(filepath)`), else the full model
            is saved (`model.save(filepath)`).
        mode: one of {auto, min, max}.
            If `save_best_only=True`, the decision
            to overwrite the current save file is made
            based on either the maximization or the
            minimization of the monitored quantity. For `val_acc`,
            this should be `max`, for `val_loss` this should
            be `min`, etc. In `auto` mode, the direction is
            automatically inferred from the name of the monitored quantity.
        period: Interval (number of epochs) between checkpoints.
        min_delta: minimum change in the monitored quantity
            to qualify as an improvement, i.e. an absolute
            change of less than min_delta, will count as no
            improvement.
        baseline: Baseline value for the monitored quantity to reach.
            Training will stop if the model doesn't show improvement
            over the baseline.
        patience: number of epochs that produced the monitored
            quantity with no improvement after which training will
            be stopped.
            Validation quantities may not be produced for every
            epoch, if the validation frequency
            (`model.fit(validation_freq=5)`) is greater than one.
    """

    def __init__(self, filepath, total_epoch, monitor='val_loss', verbose=0,
                 mode='auto', min_delta=0,
                 patience=10,baseline=None, period=1):
        super(CustomModelCheckpoint, self).__init__()
        self.monitor = monitor
        self.verbose = verbose
        self.filepath = filepath
        self.period = period
        self.epochs_since_last_save = 0
        self.min_delta = min_delta
        self.wait = 0
        self.baseline = baseline
        self.stopped_epoch = 0
        self.best_weights = None
        self.total_epoch = total_epoch
        self.times = []
        if mode not in ['auto', 'min', 'max']:
            warnings.warn('ModelCheckpoint mode %s is unknown, '
                          'fallback to auto mode.' % (mode),
                          RuntimeWarning)
            mode = 'auto'

        if mode == 'min':
            self.monitor_op = np.less
            self.best = np.Inf
        elif mode == 'max':
            self.monitor_op = np.greater
            self.best = -np.Inf
        else:
            if 'acc' in self.monitor or self.monitor.startswith('fmeasure'):
                self.monitor_op = np.greater
                self.best = -np.Inf
            else:
                self.monitor_op = np.less
                self.best = np.Inf
        if self.monitor_op == np.greater:
            self.min_delta *= 1
            recorder['mode'] = 'max'
        else:
            self.min_delta *= -1
            recorder['mode'] = 'min'

    def on_train_begin(self, logs={}):
        self.times = []
        self.wait = 0
        self.stopped_epoch = 0
        if self.baseline is not None:
            self.best = self.baseline
        else:
            self.best = np.Inf if self.monitor_op == np.less else -np.Inf

    def on_epoch_begin(self, epoch, logs={}):
        self.epoch_time_start = time.time()

    def on_epoch_end(self, epoch, logs=None):
        logs = logs or {}
        self.epochs_since_last_save += 1
        recorder['epochs'] += 1
        self.times.append(time.time() - self.epoch_time_start)
        recorder['History time'] = self.times
        recorder['estimate time for next iteration'] = np.mean(np.array(self.times[-3:])) if len(self.times) == 3 else None if len(self.times) == 0 else self.times[-1:]
        recorder['estimate time for training'] = None if len(self.times) == 0 else np.mean(np.array(self.times)) * (self.total_epoch - epoch)
        if self.epochs_since_last_save >= self.period:
            self.epochs_since_last_save = 0
            filepath = self.filepath.format(epoch=epoch + 1, **logs)
            # For the best model
            current = logs.get(self.monitor)
            recorder['history result'].append(current)
            if current is None:
                warnings.warn('Can save best model only with %s available, '
                              'skipping.' % (self.monitor), RuntimeWarning)
            else:
                if self.monitor_op(current - self.min_delta, self.best):
                    if self.verbose > 0:
                        print('\nEpoch %05d: %s improved from %0.5f to %0.5f,'
                              ' saving model to %s'
                              % (epoch + 1, self.monitor, self.best,
                                  current, filepath))
                    self.best = current
                    self.wait = 0
                    self.model.save('latest_' + filepath, overwrite=True)
                    self.best_weights = self.model.get_weights()
                else:
                    self.wait += 1
                    if self.wait >= self.patience:
                        self.stopped_epoch = epoch
                        self.model.stop_training = True
                        self.model.set_weights(self.best_weights)
                        if self.verbose > 0:
                            print('\nEpoch %05d: %s did not improve from %0.5f' %
                                  (epoch + 1, self.monitor, self.best))
        # For the latest model
        if self.verbose > 0:
            print('\nEpoch %05d: saving model to %s' % (epoch + 1, filepath))
        self.model.save(filepath, overwrite=True)
        # Update the Recorder
        with open('recorder.json', 'w') as outfile:
            json.dump(recorder, outfile)

# start fitting 
new_model.fit(x_train, y_train, batch_size=batch_size,
              validation_data=(x_val, y_val), 
              epochs=epochs,
              verbose=1,callbacks=[CustomModelCheckpoint('model.h5', recorder['total_epoch'], monitor='val_loss', verbose=0, 
                mode='auto', period=1)])
with open('_COMPLETED', 'w') as fp: 
    pass