const express = require('express');
const app = express();
// const mapRoutes = require('./routes/map');
const apiRoutes = require('./routes/index');

app.use(express.json());
app.use('/api', apiRoutes);

const PORT = process.env.PORT || 8888;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});