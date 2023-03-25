
pip install --target ./package requests-aws4auth opensearch-py inflection

cd package
zip -r ../deployment2.zip .

cd ..
zip deployment2.zip LF2.py
