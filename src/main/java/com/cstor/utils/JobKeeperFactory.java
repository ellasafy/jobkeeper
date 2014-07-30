package com.cstor.utils;

import com.cstor.JKWatcher;

public class JobKeeperFactory {
     private static volatile  JobKeeperFactory INSTANCE = null;
     private JKWatcher jkWatcher = null;
     
     private JobKeeperFactory() {
         if (getJkWatcher() == null) {
             jkWatcher = new JKWatcher(Properties.getInstance());
         }
     }
     
     public static JobKeeperFactory getInstance() {
         if (INSTANCE == null) {
             synchronized(JobKeeperFactory.class) {
                 if (INSTANCE == null) {
                     INSTANCE = new JobKeeperFactory();
                 }
             }
         }
         return INSTANCE;
     }

    public JKWatcher getJkWatcher() {
        return jkWatcher;
    }

}
