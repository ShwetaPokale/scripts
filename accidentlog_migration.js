const mysql = require('mysql2');
const { Client } = require('pg');

// MySQL Connection
const mysqlConnection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'root',
    database: 'cabomsgoi_goicar'
});

// PostgreSQL Connection
const pgClient = new Client({
    host: 'goicar.ctsyy8sie2xh.ap-south-1.rds.amazonaws.com',
    user: 'sazinga',
    password: '20^v5B80oIp^',
    database: 'goicar',
    port: 5432
});

async function migrateData() {
    try {
        // Connect to PostgreSQL
        await pgClient.connect();
        
        // Fetch and migrate data from acc_logs
        mysqlConnection.query('SELECT * FROM acc_logs', async (err, results) => {
            if (err) {
                console.error('Error fetching data from MySQL:', err);
                return;
            }
            
            for (const row of results) {
                const query = `
                    INSERT INTO accidents (
                        "accidentType", "accidentDate", "workshopName", "workshopContact", "insuranceCompanyName", 
                        "insurancePolicyNumber", "accidentBy", "driverId", "bookingId", "surveyerName", 
                        "surveyerContactNumber", "billAmount", "advanceAmount", "billDate", "billCopy", 
                        "paidAmount", "paidDate", "remarks", "insuranceAmountReceived", "insuranceReceivedDate", 
                        "createdAt", "createdBy", "updatedAt", "updatedBy", "deleted", "IPAddress", "HOST", "status", 
                        "vehicleId", "claimType"
                    ) VALUES (
                        $1, $2, $3, $4, $5, 
                        $6, $7, $8, $9, $10, 
                        $11, $12, $13, $14, $15, 
                        $16, $17, $18, $19, $20, 
                        $21, $22, $23, $24, $25, $26, $27, $28, 
                        $29, $30
                    )
                `;

                const values = [
                    row.acc_dtype || null, 
                    row.acc_log_date || null, 
                    row.acc_wshop || null, 
                    row.acc_wshop_num || null, 
                    row.acc_log_icomp || null,
                    row.acc_log_plnum || null, 
                    row.acc_log_user || null, 
                    row.acc_log_drv || null, 
                    row.acc_log_bid || null, 
                    row.acc_log_serv_nm || null,
                    row.acc_log_serv_ph || null, 
                    row.acc_log_amt && !isNaN(row.acc_log_amt) ? parseFloat(row.acc_log_amt) : null, 
                    row.acc_log_adv && !isNaN(row.acc_log_adv) ? parseFloat(row.acc_log_adv) : null, 
                    row.acc_log_bill_date || null, 
                    row.acc_bill_file || null,
                    row.acc_log_paidamt && !isNaN(row.acc_log_paidamt) ? parseFloat(row.acc_log_paidamt) : null, 
                    row.acc_log_pay_date || null, 
                    row.acc_log_remark || null, 
                    row.acc_log_ins_recd && !isNaN(row.acc_log_ins_recd) ? parseFloat(row.acc_log_ins_recd) : null, 
                    row.acc_ins_recd && !isNaN(row.acc_ins_recd) ? parseFloat(row.acc_ins_recd) : null,
                    row.acc_log_date || null, 
                    row.acc_log_user || null, 
                    row.acc_log_last || null, 
                    row.acc_log_user || null, 
                    row.acc_log_deleted || false, 
                    row.acc_log_ipaddress || null, 
                    row.acc_log_host || null, 
                    row.acc_log_status || null,
                    row.acc_log_vid && !isNaN(row.acc_log_vid) ? parseInt(row.acc_log_vid) : null, 
                    row.acc_log_type || null
                ];

                try {
                    await pgClient.query(query, values);
                    console.log('Data migrated for acc_log_id:', row.acc_log_id);
                } catch (pgErr) {
                    console.error('Error inserting data into PostgreSQL:', pgErr);
                }
            }
        });

        // Fetch and migrate data from vehicles
        let [vehiclesRows] = await mysqlConnection.promise().query('SELECT * FROM vehicles WHERE vid IN (73, 74, 75, 76, 77)');

        for (let vehicle of vehiclesRows) {

            let [modelRow] = await mysqlConnection.promise().query(
                'SELECT model_name FROM car_models WHERE model_id = ?',
                [vehicle.vmodel]
            );
            let model = modelRow && modelRow[0] ? modelRow[0].model_name : null;

            let [brandRow] = await mysqlConnection.promise().query(
                'SELECT make_name FROM car_makes WHERE make_id = ?',
                [vehicle.vmake]
            );
            let brand = brandRow && brandRow[0] ? brandRow[0].make_name : null;

            let columnMap = {
                id: vehicle.vid || null,
                vendorId: vehicle.vowner || null,
                brand: brand,
                model: model,
                registrationNumberPlate: vehicle.vname || null,
                yearOfManufacturing: vehicle.vmake_yr || null,
                fuelType: vehicle.vfuel_type || null,
                seatingCapacity: 5,
                airBags: '',
                milage: vehicle.vtotal_km || null,
                securityAmount: vehicle.vdeposite || null,
                registrationDate: vehicle.reg_date || null,
                dailyRentalPrice: 0,
                color: '',
                engineNumber: vehicle.eng_no || null,
                chasisNumber: vehicle.chasis_number || null,
                kmReading: vehicle.km_reading || null,
                isAirConditional: vehicle.ac_type || null,
                createdAt: '',
                createdBy: '',
                updatedAt: '',
                updatedBy: '',
                deleted: false,
                IPAddress: '',
                HOST: '',
                commissionType: '',
                commissionValue: 0,
                remark: '',
                status: vehicle.v_status || null,
                vehicleServiceIntervalKm: 0,
                currentFuel: vehicle.vfuel_lvl || null,
                rentalVid: vehicle.vid2 || null,
                fast_tag_id: vehicle.fast_tag_id || null,
                gallery_imgs: vehicle.gallery_imgs || null,
                max_fuel_lv: vehicle.max_fuel_lv || null,
            };

            let columns = Object.keys(columnMap).filter((key) => columnMap[key] !== null && columnMap[key] !== '');
            let values = columns.map((key) => columnMap[key]);

            let placeholders = values.map((_, index) => `$${index + 1}`).join(', ');

            let query = `
    INSERT INTO "vehicles" ("${columns.join('", "')}")
    VALUES (${placeholders})
    ON CONFLICT (id) DO NOTHING
    `;
            await pgClient.query(query, values);
            console.log(`Successfully inserted vehicle with ID: ${vehicle.vid}`);
        }

        mysqlConnection.end();
        pgClient.end();
           
    } catch (error) {
        console.error('Error during migration:', error);
        mysqlConnection.end();
        pgClient.end();
    }
}

migrateData();
