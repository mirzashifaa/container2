const express = require('express');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const app = express();
const port = 6001;
const persistant_storage_path = '/app/shifa_PV_dir'

app.use(express.json());

app.post('/calculate', (req, res) => {

    const { file, product } = req.body;
    const results = [];
    const filePath = path.join(persistant_storage_path, file);
    let fileValid = true;

    const fileContents = fs.readFileSync(filePath, 'utf-8');
    const lines = fileContents.trim().split('\n');
    const headers = lines[0].replace(/\s+$/g, "").replace(/,\s+/g, ",").split(',');
    const productIndex = headers.indexOf('product');
    const amountIndex = headers.indexOf('amount');
    console.log('headers:', headers);

    if (!headers|| headers.length!=2||productIndex === -1 || amountIndex === -1) {
      return res.status(400).json({ file, error: 'Input file not in CSV format.' });
    }

    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => {
            
            const normalizedData = {};
            for (const key in data) {
                normalizedData[key.trim()] = data[key].trim();
            }

            if (normalizedData.product && !isNaN(parseInt(normalizedData.amount, 10))) {
                if (normalizedData.product === product) {
                    results.push(parseInt(normalizedData.amount, 10));
                }
            } else {
                console.log('Invalid row detected:', normalizedData);
                fileValid = false;
            }
        })
        .on('end', () => {
            const sum = results.reduce((acc, val) => acc + val, 0);
            if (!fileValid) {
                res.status(400).json({ error: "Input file not in CSV format.", file });
            } else {
                res.json({ file, sum });
            }
        })
        .on('error', (err) => {
            console.error(`Error processing file: ${err}`);
            res.status(500).json({ error: "Error processing file.", file });
        });
});

app.listen(port, () => {
    console.log(`Container 2 listening on port ${port}`);
});
