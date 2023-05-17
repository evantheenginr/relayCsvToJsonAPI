require('dotenv').config();
const { RateLimiter } = require('limiter');
const csv = require('csvtojson');
const { default: axios } = require('axios');
const limiter = new RateLimiter({ tokensPerInterval: 9, interval: 2000 });

//Define the workflows here, one per endpoint/csv combo
const workflows = [
    {   
        description: 'load base transactions from csv to lb',
        data: async () => {
            return csv().fromFile(process.env.BASE_TRANS_CSV || './basetrans.csv')
        },
        action: async (doc) => {
            await csvToJsonToPost(`${process.env.LB_URL}${process.env.BASE_TRANS_URL}`, doc)
        }
    },
    //Add next workflow here, like for contracts or something
];

//Generic csv to json with post
async function csvToJsonToPost(url, doc){
    await limiter.removeTokens(1);
    log('basetrans', 'info', 'action triggered');
    //TODO: validate the doc and skip if invalid
    axios.post(url, doc, {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + process.env.AUTH_TOKEN
        }
    }).then((res) => {
        log('basetrans', 'info', 'action completed', res.status, res.data);
    }).catch((err) => {
        log('basetrans', 'error', 'action failed', err.message, err.response.data);
        if(err.response.status === 401){
            log('basetrans', 'error', 'skip remaining data due to unauthorized', err.message, err.response.data);
            throw new Error('skip remaining data due to unauthorized');
        }
    });
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
        log('controller', 'info', `Running job`, workflow.description)
        if(workflow?.data !== undefined && workflow?.action !== undefined){
            log('controller', 'info', 'workflow has data and action, running', workflow.description)
            workflow
                .data()
                .then((row) => {
                    row.map(async (doc) => {
                        log('controller', 'info', 'job record found', workflow.description, doc)
                        await workflow.action(doc);
                        log('controller', 'info', 'job record processed', workflow.description)
                    });
                })
                .catch(err => {
                    log('controller', 'error', 'job failed', workflow.description, err.message)
                });
        }
    }));
}

//Go!
(async () => {
    if(process.env.LB_URL === undefined || process.env.LB_URL === '' || process.env.AUTH_TOKEN === undefined || process.env.AUTH_TOKEN === ''){
        throw new Error('Not setup!  Look at your .env file.  If you have not created a .env file, please consult .env.sample to get started.');
    }
    //Mark the log every 15 seconds in case this is long running and we want to know if its stuck
    const interval = setInterval(() => {
        console.log(`> MARK --- ${new Date().toISOString()} ---`)
    }, 1000 * 15);
    console.log(`> Ready, --- ${new Date().toISOString()} ---`)
    await execute();
    clearInterval(interval);
})();