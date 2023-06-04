require('dotenv').config();
const { RateLimiter } = require('limiter');
const csv = require('csvtojson');
const { default: axios } = require('axios');
const limiter = new RateLimiter({ tokensPerInterval: 1, interval: 1 });

//Promises for dependancies, as needed
const dependancies = {
    taxrateheader: resolver(),
    exchangeheader: resolver(),
};

//Define the workflows here, one per endpoint/csv combo
const workflows = [
    {   
        enabled: true,
        description: 'bulk load base transactions from csv to lb',
        data: async () => {
            const data = await csv().fromFile(process.env.BASE_TRANS_CSV || './basetrans.csv')
            const chunkSize = 500;
            const res =  await Promise.all(data.reduce((resultArray, item, index) => {
                const chunkIndex = Math.floor(index/chunkSize)
                if(!resultArray[chunkIndex]) {
                    resultArray[chunkIndex] = [] // start a new chunk
                }
                //Note the slight remap here to avoid iterating again in the action
                resultArray[chunkIndex].push({data: item})
                return resultArray
            }
            , []))
            return res;
        },
        action: async (doc) => {
            //TODO: we have remapped this to the bulk adaptor, but it's not working yet
            //there is no documentation on that endpoint, so we had to guess.  we guessed wrong.
            const mapped = { dataList: doc };
            await csvToJsonToAPI('post', `${process.env.LB_URL}${process.env.BASE_TRANS_BULK_URL || '/basetrans/v1/basetransaction'}`, mapped)
        }
    },
    {   
        enabled: false,
        description: 'load base transactions from csv to lb',
        data: async () => {
            return csv().fromFile(process.env.BASE_TRANS_CSV || './basetrans.csv')
        },
        action: async (doc) => {
            //Note: Can do remapping right here if needed if the CSV columns don't match the JSON LB model
            await csvToJsonToAPI('post', `${process.env.LB_URL}${process.env.BASE_TRANS_URL || '/transdata/v1/transaction/base'}`, doc)
        }
    },
    {   
        enabled: false,
        description: 'load tax rate headers from csv to lb',
        data: async () => {
            return csv().fromFile(process.env.TAX_HEADER_CSV || './taxheaders.csv')
        },
        action: async (doc) => {
            const mapped = { dataList: [{ data: doc }] };
            await csvToJsonToAPI('post', `${process.env.LB_URL}${process.env.TAX_RATE_HEADER_URL || '/contract/v1/mqs/taxrateheader'}`, mapped)
        },
        complete: dependancies.taxrateheader.resolver
    },
    {   
        enabled: false,
        description: 'load tax rate details from csv to lb',
        data: async () => {
            return csv().fromFile(process.env.TAX_DTL_CSV || './taxdetails.csv')
        },
        action: async (doc) => {
            const id = await getID('post', `${process.env.LB_URL}${process.env.TAX_RATE_HEADER_URL || '/contract/v1/mqs/taxrateheader'}/query?size=1&page=0`, [{key: 'taxRateId', value: doc.headerKey, operation: 'eq'}])
            const mapped = { dataList: [{ data: {...doc, headerKey: id.response[0].rateKey} }] };
            await csvToJsonToAPI('post', `${process.env.LB_URL}${process.env.TAX_RATE_DETAIL_URL || '/contract/v1/mqs/taxratedetails'}`, mapped)
        },
        depends: dependancies.taxrateheader.promise
    },
    {   
        enabled: false,
        description: 'load exchange rate headers from csv to lb',
        data: async () => {
            return csv().fromFile(process.env.EXCHANGE_RATE_HEADER_CSV || './exchangeheaders.csv')
        },
        action: async (doc) => {
            const mapped = { dataList: [{ data: doc }] };
            await csvToJsonToAPI('post', `${process.env.LB_URL}${process.env.EXCHANGE_RATE_HEADER_URL || '/contract/v1/mqs/exchangeRateHeader'}`, mapped)
        },
        complete: dependancies.exchangeheader.resolver
    },
    {   
        enabled: false,
        description: 'load exchange rate details from csv to lb',
        data: async () => {
            return csv().fromFile(process.env.EXCHANGE_RATE_DTL_CSV || './exchangedetails.csv')
        },
        action: async (doc) => {
            const id = await getID('post', `${process.env.LB_URL}${process.env.EXCHANGE_RATE_HEADER_URL || '/contract/v1/mqs/exchangeRateHeader'}/query?size=1&page=0`, [{key: 'exchangeRateId', value: doc.headerKey, operation: 'eq'}])
            const mapped = { dataList: [{ data: {...doc, headerKey: id.response[0].headerKey} }] };
            await csvToJsonToAPI('post', `${process.env.LB_URL}${process.env.EXCHANGE_RATE_DETAIL_URL || '/contract/v1/mqs/exchangeRateDetails'}`, mapped)
        },
        depends: dependancies.exchangeheader.promise
    },

    {   
        enabled: false,
        description: 'load clients from csv to lb',
        data: async () => {
            return csv().fromFile(process.env.CLIENT_CSV || './client.csv');
        },
        action: async (doc) => {
            const mapped = { dataList: [{ data: doc }] };
            await csvToJsonToAPI('post', `${process.env.LB_URL}${process.env.CLIENT_URL || '/contract/v1/mqs/client'}`, mapped);
        }
    },
    //Add next workflow here, like for contracts or something
];

async function authenticate(){
    const res = await axios({ method: 'post', url: process.env.AUTH_URL, data: {
            grant_type: 'client_credentials',
            client_id: process.env.AUTH_CLIENT_ID,
            client_secret: process.env.AUTH_CLIENT_SECRET,
            scope: process.env.AUTH_SCOPE
        },
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
    });
    process.env.AUTH_TOKEN = res.data.access_token;
}

//Lookup ID for header to insert details
async function getID(method, url, data){
    await limiter.removeTokens(1);
    log('getID', 'info', 'lookuping up id for key');
    try {
        const res = await axios({ method, url, data,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + process.env.AUTH_TOKEN
            }
        })
        log('getID', 'info', 'lookup completed', res.status, res.data);
        return res.data;
    } catch(err) {
        log('getID', 'error', 'lookup failed', err.message, err.response.data);
        if(err.response.status === 401){
            log('getID', 'error', 'skip remaining data due to unauthorized', err.message, err.response.data);
            throw new Error('skip remaining data due to unauthorized');
        }
    }
}

//Generic csv to json with post
async function csvToJsonToAPI(method, url, data){
    await limiter.removeTokens(1);
    log('csvToJsonToAPI', 'info', 'action triggered');
    try {
        const res = await axios({ method, url, data,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + process.env.AUTH_TOKEN
            }
        })
        log('csvToJsonToAPI', 'info', 'action completed', res.status, res.data);
    } catch(err) {
        log('csvToJsonToAPI', 'error', 'action failed', err.message, err.response.data);
        if(err.response.status === 401){
            log('csvToJsonToAPI', 'error', 'skip remaining data due to unauthorized', err.message, err.response.data);
            throw new Error('skip remaining data due to unauthorized');
        }
    }
}

//Generic promise to wait for a dependancy
function resolver(){
    let resolver;
    return { promise: new Promise((resolve, reject) => { resolver = resolve }), resolver };
}

//Generic logging with on/off switch
function log(workflow, level, msg, key, data){
    if(process.env.CONSOLE_DEBUG === '1'){
        const func = level === 'error' ? console.error : level === 'warn' ? console.log : console.info;
        func(`[${workflow}] [${key}] ${msg}`, data!==undefined?data:'');
    }
}

//Mainline
async function execute(){
    log('controller', 'info', 'workflows jobs are initializing')
    await Promise.allSettled(workflows.map(async (workflow) => {
        if(workflow?.data !== undefined && workflow?.action !== undefined && workflow.enabled === true){
            log('controller', 'info', `Running job`, workflow.description)
            log('controller', 'info', 'workflow has data and action, running', workflow.description)
            if(workflow.depends !== undefined){
                log('controller', 'info', 'workflow has dependancy, waiting', workflow.description)
                await workflow.depends;
            }
            try {
                const data = await workflow.data()
                await Promise.allSettled(data.map(async (doc) => {
                    log('controller', 'info', 'job record found', workflow.description, doc)
                    await workflow.action(doc);
                    log('controller', 'info', 'job record processed', workflow.description)
                }));
            } catch (err) {
                log('controller', 'error', 'job failed', workflow.description, err.message)
            } finally {
                if(workflow.complete !== undefined){
                    workflow.complete();
                    log('controller', 'info', 'job complete, releasing dependant jobs', workflow.description)
                }
            }
        }else if(workflow.enabled === false && workflow.complete !== undefined){
            workflow.complete();
            log('controller', 'info', 'disabled job complete, releasing dependant jobs', workflow.description)
        }
    }));
}

//Go!
(async () => {
    if(process.env.LB_URL === undefined || 
        process.env.LB_URL === '' || 
        process.env.AUTH_URL === undefined || 
        process.env.AUTH_SCOPE === undefined || 
        process.env.AUTH_CLIENT_ID === undefined || 
        process.env.AUTH_CLIENT_SECRET === ''){
        throw new Error('Not setup!  Look at your .env file.  If you have not created a .env file, please consult .env.sample to get started.');
    }
    //Refresh the auth token every 45 minutes
    const refresh = setInterval(async () => {
        log('global', 'info', 'refreshing authentication', 'auth')
        await authenticate();
        log('global', 'info', 'authentication refreshed', 'auth', `\nBearer ${process.env.AUTH_TOKEN}`)
    }, 1000 * 60 * 45);
    log('global', 'info', 'starting authentication', 'auth')
    await authenticate();
    log('global', 'info', 'authentication complete', 'auth', `\nBearer ${process.env.AUTH_TOKEN}`)
    //Mark the log every 15 seconds in case this is long running and we want to know if its stuck
    const mark = setInterval(() => {
        console.log(`> MARK --- ${new Date().toISOString()} ---`)
    }, 1000 * 15);
    console.log(`> Ready, --- ${new Date().toISOString()} ---`)
    await execute();
    clearInterval(mark);
    clearInterval(refresh);
})();