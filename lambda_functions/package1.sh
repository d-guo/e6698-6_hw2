
pip install --target ./package requests-aws4auth opensearch-py inflection

cd package
zip -r ../deployment1.zip .

cd ..
zip deployment1.zip LF1.py
