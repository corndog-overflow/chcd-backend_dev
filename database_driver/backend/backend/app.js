const express = require('express');
const app = express();
const mapRoutes = require('./routes/map');

app.use(express.json());
app.use('/api/map', mapRoutes);

const PORT = process.env.PORT || 8888;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});