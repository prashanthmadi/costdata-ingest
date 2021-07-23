module.exports = async function (context, req) {
    context.log('Started ingest Process');

    // const name = (req.query.name || (req.body && req.body.name));
    // const responseMessage = name
    //     ? "Hello, " + name + ". This HTTP triggered function executed successfully."
    //     : "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.";


    const { DefaultAzureCredential } = require("@azure/identity");
    const { ConsumptionManagementClient } = require("@azure/arm-consumption");
    const { Connection, TYPES } = require('tedious');

    const subscriptionId = "b7442038-57ac-4487-9645-0a07e04d2491";
    const config = {
        server: 'xxxx.database.windows.net',
        authentication: {
            type: 'default',
            options: {
                userName: 'xxxxx',
                password: 'xx@@'
            }
        },
        options: {
            "database": "xxx",
            encrypt: true
        }
    };

    const credentials = new DefaultAzureCredential();
    const client = new ConsumptionManagementClient(credentials, subscriptionId);
    const connection = new Connection(config);
    const table = 'cost_test1';


    async function getData() {
        const expand = "properties/meterDetails";
        const top = 8;
        var result = await client.usageDetails.list({ expand, top });
        processPageResult(result);
        var nextPage = result.nextLink;
        while (nextPage != null) {
            result = await client.usageDetails.listNext(nextPage);
            processPageResult(result);
            nextPage = result.nextLink;
        }
    }

    function processPageResult(result) {
        //  context.log(result);   
        loadBulkData(result);
    }

    function loadBulkData(result) {
        console.log("moving data to SQL")

        var options = { keepNulls: true };

        const bulkLoad = connection.newBulkLoad(table, options, (err, rowCont) => {
            if (err) {
                throw err;
            }
            console.log('rows inserted :', rowCont);
            console.log('DONE!');
        });

        // setup columns
        bulkLoad.addColumn('id', TYPES.VarChar, { nullable: false });
        bulkLoad.addColumn('tags', TYPES.NVarChar, { length: 'max', nullable: true });
        bulkLoad.addColumn('usageStart', TYPES.DateTime2, { nullable: false });
        bulkLoad.addColumn('usageEnd', TYPES.DateTime2, { nullable: false });
        bulkLoad.addColumn('instanceName', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('instanceId', TYPES.NVarChar, { length: 'max', nullable: true });
        bulkLoad.addColumn('instanceLocation', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('currency', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('usageQuantity', TYPES.Decimal, { precision: 38, scale: 29, nullable: false });
        bulkLoad.addColumn('pretaxCost', TYPES.Decimal, { precision: 38, scale: 29, nullable: false });
        bulkLoad.addColumn('meterId', TYPES.UniqueIdentifier, { nullable: true });
        bulkLoad.addColumn('meterName', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('meterCategory', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('meterSubCategory', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('unit', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('meterLocation', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('serviceName', TYPES.NVarChar, { length: 'max', nullable: true });
        bulkLoad.addColumn('serviceTier', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('subscriptionGuid', TYPES.VarChar, { nullable: false });
        bulkLoad.addColumn('subscriptionName', TYPES.VarChar, { nullable: false });
        bulkLoad.addColumn('consumedService', TYPES.VarChar, { nullable: true });
        bulkLoad.addColumn('offerId', TYPES.VarChar, { length: 50, nullable: true });

        // add rows
        result.forEach(element => {
            bulkLoad.addRow({
                id: element.id, tags: JSON.stringify(element.tags),
                usageStart: element.usageStart, usageEnd: element.usageEnd,
                instanceName: element.instanceName, instanceId: element.instanceId,
                instanceLocation: element.instanceLocation, currency: element.currency,
                usageQuantity: element.usageQuantity, pretaxCost: element.pretaxCost,
                meterId: element.meterId, meterName: element.meterDetails.meterName,
                meterCategory: element.meterDetails.meterCategory, meterSubCategory: element.meterDetails.meterSubCategory,
                unit: element.meterDetails.unit, meterLocation: element.meterDetails.meterLocation,
                serviceName: element.meterDetails.serviceName, serviceTier: element.meterDetails.serviceTier,
                subscriptionGuid: element.subscriptionGuid, subscriptionName: element.subscriptionName,
                consumedService: element.consumedService, offerId: element.offerId
            })
        });

        // perform bulk insert
        connection.execBulkLoad(bulkLoad);
    }


    connection.connect((err) => {
        if (err) {
            context.log('Connection Failed');
            throw err;
        }
        getData();
    });







    context.res = {
        // status: 200, /* Defaults to 200 */
        body: "worked"
    };
}