{application,mm_mq,
             [{description,"Message Queue Tools App"},
              {vsn,"0.0.1"},
              {registered,[]},
              {applications,[kernel,stdlib,mm_config,mm_log]},
              {mod,{mm_mq_app,[]}},
              {env,[]},
              {modules,[mm_mq,mm_mq_app,mm_mq_sup,mq,mq_pool,
                        mq_pool_worker]}]}.
